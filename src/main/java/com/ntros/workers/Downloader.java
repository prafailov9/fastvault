package com.ntros.workers;

import com.ntros.data.CancellationToken;
import com.ntros.data.DeviceAddress;
import com.ntros.data.PathType;
import com.ntros.data.RuntimeContext;
import com.ntros.data.platform.PlatformState;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
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
  private final String baseUri;
  // stops threads from pointlessly downloading the same file
  // contains filesnames that are currently being downloaded
  // files are removed once the files is fully acked on the targer machine
  private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
  // allow at most 5 VTs to download simultaneously.
  // Server-side workers are limited, so 1K concurrent downloaders will create a bottleneck.
  private final Semaphore semaphore;

  public Downloader(RuntimeContext runtimeContext, HttpClient client) {
    this.runtimeContext = runtimeContext;
    this.client = client;
    token = runtimeContext.workersToken();
    platformState = runtimeContext.platformState();
    DeviceAddress targetDeviceAddress = runtimeContext.targetDeviceAddress();
    baseUri = String.format("http://%s:%s", targetDeviceAddress.host(), targetDeviceAddress.port());
    semaphore = new Semaphore(runtimeContext.dwPermits());
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

      // one flat listing covers the whole tree
      submitFiles(getFiles(), downloadDirectory);
    }
  }

  private void submitFiles(Set<String> relPaths, Path downloadDirectory) {
    if (!relPaths.isEmpty()) {
      log.debug("listed {}", relPaths);
    }
    // delegate download + write to VTs
    for (var f : relPaths) {
      // if a listed file is in the set, skip it since its already being processed
      if (!inFlight.add(f)) {
        continue;
      }
      // acquire inside VT so the downloader is not blocked.
      // on large number of files to download(n = 1000), 1K VTs will be
      // created, only N of them allowed to download.
      // The rest wait.
      // VTs waiting is nearly free because they dont pin OS threads.
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  semaphore.acquire();
                  try {
                    download(f, downloadDirectory).ifPresent(dest -> ack(f));
                  } finally {
                    semaphore.release(); // can only run if the acquire above returned
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } finally {
                  inFlight.remove(f); // pairs with the add() in the loop, runs no matter what
                }
              });
    }
  }

  // tells the server downloading of this file is finished. Server moves it out/ -> sent/
  private void ack(String relPath) {
    var req =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    baseUri
                        + "/ack?filename="
                        + URLEncoder.encode(relPath, StandardCharsets.UTF_8)))
            .timeout(Duration.ofSeconds(10))
            .POST(HttpRequest.BodyPublishers.noBody()) // POST on state change.
            .build();
    try {
      var res = client.send(req, HttpResponse.BodyHandlers.ofString());
      if (res.statusCode() != 200) {
        log.warn("ack {} failed: HTTP {} {}", relPath, res.statusCode(), res.body());
      }
    } catch (IOException e) {
      log.warn("ack {} failed: {}", relPath, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * One request returns every undelivered file under the source's out/, as '/'-separated relative
   * paths. May contain inFlight files too, until they are acked. Also serves as the liveness check:
   * an unreachable peer fails here, so there is no separate /healthcheck call.
   */
  private Set<String> getFiles() {
    var req =
        HttpRequest.newBuilder()
            .uri(URI.create(String.format("%s/files", baseUri)))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();

    try {
      var res = client.send(req, HttpResponse.BodyHandlers.ofLines());
      if (res.statusCode() != 200) {
        log.warn("list failed: HTTP {}", res.statusCode());
        return Set.of();
      }
      // an empty body -> an empty set: an empty out/ is a normal answer, not an error
      return res.body().filter(s -> !s.isBlank()).collect(Collectors.toSet());
    } catch (IOException e) {
      // message only, no stack trace: when the peer is down this fires every cycle,
      // and a full trace per cycle is how log files die
      log.warn("list failed: {}", e.getMessage());
      return Set.of();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Set.of();
    }
  }

  private Set<String> getPaths(PathType pathType) {
    String pathName = pathType.name().toLowerCase();
    var req =
        HttpRequest.newBuilder()
            .uri(URI.create(String.format("%s/%s", baseUri, pathName)))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();

    try {
      var res = client.send(req, HttpResponse.BodyHandlers.ofLines());
      if (res.statusCode() == 204) {
        log.info("No {}s for transfer at source", pathName);
        return Set.of();
      }
      return res.body().collect(Collectors.toSet());
    } catch (IOException | InterruptedException e) {
      log.error("failed during get-{}s request", pathName, e);
    }
    return Set.of();
  }

  /**
   * Downloads to .tmp under a random name, verifies the size, then atomically moves into place
   * under in/, creating any parent directories the relative path implies. Overwrites existing with
   * downloaded on same-name. For meaningfully different files with the same name, should send more
   * information.
   */
  private Optional<Path> download(String relPath, Path downloadDirectory) {
    var req =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    baseUri
                        + "/download?filename="
                        + URLEncoder.encode(relPath, StandardCharsets.UTF_8)))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();
    Path tmp = Paths.get(platformState.homeDir(), runtimeContext.basedir(), ".tmp");

    boolean created = createDirIfNotExist(tmp);
    if (!created) {
      return Optional.empty();
    }
    // relPath may contain slashes now, so it cannot be part of a flat staging name;
    // a UUID alone is unique, and the mapping lives in the debug log
    Path part = tmp.resolve(UUID.randomUUID() + ".part");
    log.debug("staging {} as {}", relPath, part.getFileName());

    try {
      var res = client.send(req, HttpResponse.BodyHandlers.ofFile(part));
      if (res.statusCode() != 200) {
        log.debug("download {} refused: HTTP {}", relPath, res.statusCode());
        return Optional.empty();
      }
      long expected = res.headers().firstValueAsLong("Content-Length").orElse(-1);
      if (expected >= 0 && Files.size(part) != expected) {
        log.warn("{}: got {} bytes, expected {}", relPath, Files.size(part), expected);
        return Optional.empty();
      }

      Path destination = downloadDirectory.resolve(relPath);
      // folders materialize as a byproduct of writing files
      Files.createDirectories(destination.getParent());
      // atomic works only if both files are on the same fs; .tmp and in/ share the vault
      Files.move(part, destination, StandardCopyOption.ATOMIC_MOVE);
      log.info("{} downloaded", relPath);
      return Optional.of(destination);
    } catch (IOException e) {
      log.error("failed during download of {}", relPath, e);
      return Optional.empty();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Optional.empty();
    } finally {
      try {
        Files.deleteIfExists(part);
      } catch (IOException ignore) {
        // best-effort cleanup; a startup sweep of .tmp is the backstop
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
