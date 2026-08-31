package com.ntros.workers;

import com.ntros.channel.MessageChannel;
import com.ntros.data.CancellationToken;
import com.ntros.data.DeviceAddress;
import com.ntros.data.RuntimeContext;
import com.ntros.data.platform.PlatformState;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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
   *      If yes -> download and store in local in dir.
   *
   *
   * HttpRequest.BodyPublishers.ofFile( Path.of("data.bin") )
   *  </pre>
   */
  @Override
  public void run() {
    Path source =
        Paths.get(platformState.homeDir(), runtimeContext.basedir(), runtimeContext.outgoing());
    while (!token.isCancelled()) {
      if (waitForDelay()) {
        return;
      }
      if (!platformState.isLeader().get()) {
        continue;
      }

      // Source Machine flow(MAC/PC)
      // 1. connect to source machine
      if (!checkLiveSourceMachine()) {
        continue;
      }

      // 2. Read available files
      var filenames = getFiles();
      if (filenames.isEmpty()) {
        continue;
      }

      // 3. delegate download to VTs. VTs write to a file channel, saver pool reads.
      for (var f : filenames) {
        Thread.ofVirtual()
            .start(
                () -> {
                  Path downloaded = download(f, source);
                  write(downloaded);
                  // TODO: probably should remove FileSaver
                });
      }
    }
  }

  private void write(Path f) {
    if (f == null) {
      return;
    }
    try {
      // TODO: figure out how to create a file at target dir
      Files.createFile(f);
    } catch (IOException e) {
      log.error("Could not create file {}", f.getFileName(), e);
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
      if (response.statusCode() != 200) {
        log.info("Server is {}", response.body());
        return false;
      }
      return true;
    } catch (IOException | InterruptedException e) {
      return false;
    }
  }

  private List<String> getFiles() {
    var req =
        HttpRequest.newBuilder().uri(URI.create(String.format("%s/files", baseUri))).GET().build();

    try {
      var res = client.send(req, HttpResponse.BodyHandlers.ofLines());
      if (res.statusCode() != 200) {
        log.info("Could not read files at source machine.");
        return List.of();
      }
      return res.body().toList();
    } catch (IOException | InterruptedException e) {
      log.error("failed during request", e);
    }
    return List.of();
  }

  private Path download(String filename, Path downloadDirectory) {
    var req =
        HttpRequest.newBuilder()
            .uri(URI.create(String.format("%s/download", baseUri)))
            .header("filename", filename)
            .GET()
            .build();

    try {
      var res = client.send(req, HttpResponse.BodyHandlers.ofFileDownload(downloadDirectory));
      if (res.statusCode() != 200) {
        log.info("Failed to download {} file from source machine.", filename);
        return null;
      }
      return res.body();
    } catch (IOException | InterruptedException e) {
      log.error("failed during request", e);
    }
    return null;
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
