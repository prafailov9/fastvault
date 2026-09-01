package com.ntros.workers;

import com.ntros.channel.MessageChannel;
import com.ntros.data.CancellationToken;
import com.ntros.data.DeviceAddress;
import com.ntros.data.RuntimeContext;
import com.ntros.data.platform.PlatformState;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Continuously reads a directory from a source machine. If any files are found, downloads them to
 * "ingoingDir".
 */
public class Downloader implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(Downloader.class);
  private final RuntimeContext runtimeContext;

  private final CancellationToken token;
  private final PlatformState platformState;
  private final HttpClient client;
  private final DeviceAddress targetDeviceAddress;
  private final String baseUri;
  private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

  public Downloader(RuntimeContext runtimeContext, HttpClient client) {
    this.runtimeContext = runtimeContext;
    this.client = client;
    token = runtimeContext.workersToken();
    platformState = runtimeContext.platformState();
    targetDeviceAddress = runtimeContext.targetDeviceAddress();
    baseUri = String.format("http://%s:%s", targetDeviceAddress.host(), targetDeviceAddress.port());
  }

  /**
   *
   *
   * <pre>
   * PC/MAC Leader:
   *  a) phone: pc checks its own mailbox(later version), downloads from there
   *  b) mac: checks if mac is live first, then asks mac for any files in its out folder.
   *      If yes -> download and store in local dir.
   *
   *
   * HttpRequest.BodyPublishers.ofFile( Path.of("data.bin") )
   *  </pre>
   */
  @Override
  public void run() {
    Path downloadDirectory =
        Paths.get(platformState.homeDir(), runtimeContext.basedir(), runtimeContext.ingoing());
    boolean created = createDirIfNotExist(downloadDirectory);
    if (!created) {
      return;
    }
    while (!token.isCancelled()) {
      if (waitForDelay()) {
        return;
      }
      if (!platformState.isLeader().get()) {
        continue;
      }

      log.info("Downloader running");
      // Source Machine flow(MAC/PC)
      // 1. connect to source machine
      if (!checkLiveSourceMachine()) {
        continue;
      }
      log.info("Target live. Listing files");
      // 2. Read available files
      var filenames = getFiles();
      if (filenames.isEmpty()) {
        log.info("No files found");
        continue;
      }
      log.info("Downloading files");
      // 3. delegate download + write to VTs
      for (var f : filenames) {
        if (!inFlight.add(f)) {
          continue;
        }
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    download(f, downloadDirectory).ifPresent(this::ack);
                  } finally {
                    inFlight.remove(f);
                  }
                });
      }
    }
  }

  // tells the server dwonloading for this file is finished. Move it from its out/ to sent/  dir
  private void ack(Path p) {
    String name = p.getFileName().toString();
    var req =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    baseUri + "/ack?filename=" + URLEncoder.encode(name, StandardCharsets.UTF_8)))
            .timeout(Duration.ofSeconds(10))
            .POST(
                HttpRequest.BodyPublishers
                    .noBody()) // state change → POST; your server doesn't check the method, so this
            // just works
            .build();
    try {
      var res = client.send(req, HttpResponse.BodyHandlers.ofString());
      if (res.statusCode() != 200) {
        log.warn("ack {} failed: HTTP {} {}", name, res.statusCode(), res.body());
      }
    } catch (IOException e) {
      log.warn("ack {} failed: {}", name, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private boolean checkLiveSourceMachine() {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(String.format("%s/healthcheck", baseUri)))
            .GET()
            .build();
    HttpResponse<String> response;
    try {
      response = client.send(request, HttpResponse.BodyHandlers.ofString());
      log.info("Target Machine is {}", response.body());
      return response.statusCode() == 200;
    } catch (IOException | InterruptedException e) {
      log.error("Could not send request to {}. Error: {}", baseUri, e.getMessage());
      return false;
    }
  }

  private Set<String> getFiles() {
    var req =
        HttpRequest.newBuilder().uri(URI.create(String.format("%s/files", baseUri))).GET().build();

    try {
      var res = client.send(req, HttpResponse.BodyHandlers.ofLines());
      if (res.statusCode() != 200) {
        log.info("Could not read files at source machine.");
        return Set.of();
      }
      return res.body().collect(Collectors.toSet());
    } catch (IOException | InterruptedException e) {
      log.error("failed during get-files request", e);
    }
    return Set.of();
  }

  /** Downloads file to a tmp dir first, then moves to destination */
  private Optional<Path> download(String filename, Path downloadDirectory) {
    var req =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    baseUri
                        + "/download?filename="
                        + URLEncoder.encode(filename, StandardCharsets.UTF_8)))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();
    Path tmp = Paths.get(platformState.homeDir(), runtimeContext.basedir(), ".tmp");

    boolean created = createDirIfNotExist(tmp);
    if (!created) {
      return Optional.empty();
    }
    Path part = tmp.resolve(filename + "." + UUID.randomUUID() + ".part");

    try {
      // currently only writing the size of the file.
      var res = client.send(req, HttpResponse.BodyHandlers.ofFile(part));
      if (res.statusCode() != 200) {
        log.info("Failed to download {} from source machine. HTTP {}", filename, res.statusCode());

        return Optional.empty();
      }
      long expected = res.headers().firstValueAsLong("Content-Length").orElse(-1);
      if (expected >= 0 && Files.size(part) != expected) {
        log.warn("{}: got {} bytes, expected {}", filename, Files.size(part), expected);
        return Optional.empty();
      }

      log.info("{} downloaded", filename);
      Path destination = downloadDirectory.resolve(filename);
      Files.move(part, destination, StandardCopyOption.ATOMIC_MOVE);
      return Optional.of(destination);
    } catch (IOException e) {
      log.error("failed during download request", e);
      return Optional.empty();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Optional.empty();
    } finally {
      try {
        Files.deleteIfExists(part);
      } catch (IOException ign) {

      }
    }
  }

  private boolean createDirIfNotExist(Path f) {
    try {
      Files.createDirectories(f);
      return true;
    } catch (IOException e) {
      log.error("Could not create download directory {}", f, e);
      return false;
    }
  }

  private boolean waitForDelay() {
    try {
      Thread.sleep(runtimeContext.dwDelayMs());
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted while waiting");
      return true;
    }
  }
}
