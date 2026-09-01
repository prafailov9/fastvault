package com.ntros.server;

import com.ntros.Shutdownable;
import com.ntros.data.RuntimeContext;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServerWrapper implements Server, Shutdownable {

  private static final Logger log = LoggerFactory.getLogger(HttpServerWrapper.class);
  private final HttpServer httpServer;
  private final RuntimeContext runtimeContext;

  public HttpServerWrapper(RuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
    int port = runtimeContext.platformState().deviceAddress().port();
    httpServer = createServer(port);
    attachHealthEndpoint();
    attachGetFilesEndpoint();
    attachDownloadEndpoint();
    attachElectEndpoint();
    attachDemoteEndpoint();


    // TODO: add GET /leadership, returning "LEADER"/"FOLLOWER" based on ps.isLeader


    httpServer.start();
    log.info("Server live on port: {}", port);
  }

  // server does not touch workers token
  @Override
  public HttpServer getServer() {
    return httpServer;
  }

  @Override
  public void shutdown() {
    httpServer.stop(10);
    log.info("Server shutdown");
  }

  private HttpServer createServer(int port) {
    try {
      return HttpServer.create(new InetSocketAddress(port), 0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void attachHealthEndpoint() {
    httpServer.createContext(
        "/healthcheck",
        exchange -> {
          Response healthResponse = buildHealthResponse();
          var bytes = healthResponse.responseBytes;
          exchange.sendResponseHeaders(healthResponse.code, bytes.length);

          try (var out = exchange.getResponseBody()) {
            out.write(bytes);
          }
        });
  }

  private void attachGetFilesEndpoint() {
    httpServer.createContext(
        "/files",
        exchange -> {
          Path outDir =
              Paths.get(
                  runtimeContext.platformState().homeDir(),
                  runtimeContext.basedir(),
                  runtimeContext.outgoing());
          log.info("received get-files request. Reading files from {}", outDir.toAbsolutePath());

          log.info("outDir = {}", outDir.toAbsolutePath());
          log.info("exists = {}", Files.exists(outDir));
          log.info("isDirectory = {}", Files.isDirectory(outDir));

          try {
            List<String> filenames;

            try (var files = Files.list(outDir)) {
              filenames =
                  files
                      .filter(Files::isRegularFile)
                      .map(path -> path.getFileName().toString())
                      .toList();
            }

            byte[] responseBytes;

            if (filenames.isEmpty()) {
              String payload = "No files available for download";
              responseBytes = payload.getBytes(StandardCharsets.UTF_8);

              exchange.sendResponseHeaders(404, responseBytes.length);
              log.info(payload);
            } else {
              String payload = String.join("\n", filenames);
              responseBytes = payload.getBytes(StandardCharsets.UTF_8);

              exchange.sendResponseHeaders(200, responseBytes.length);
            }
            log.info("Sending files batch");
            try (var out = exchange.getResponseBody()) {
              out.write(responseBytes);
            }

          } catch (Exception e) {
            log.error("Failed to list files in {}", outDir.toAbsolutePath(), e);

            String payload = "Internal server error";
            byte[] responseBytes = payload.getBytes(StandardCharsets.UTF_8);

            exchange.sendResponseHeaders(500, responseBytes.length);

            try (var out = exchange.getResponseBody()) {
              out.write(responseBytes);
            }
          }
        });
  }

  private void attachDownloadEndpoint() {
    httpServer.createContext(
        "/download",
        exchange -> {
          var filename = Path.of(exchange.getRequestHeaders().get("filename").getFirst());
          // check if f exist
          Path outDir =
              Paths.get(
                  runtimeContext.platformState().homeDir(),
                  runtimeContext.basedir(),
                  runtimeContext.outgoing());
          Path file = outDir.resolve(filename);
          log.info("Download requested: {}", filename);
          log.info("Resolved path: {}", file.toAbsolutePath());

          byte[] responseBytes;
          if (Files.notExists(file)) {
            responseBytes =
                String.format("File %s not found", filename).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(404, responseBytes.length);

          } else {
            responseBytes = Files.readAllBytes(file);
            exchange.sendResponseHeaders(200, responseBytes.length);
          }

          try (var out = exchange.getResponseBody()) {
            out.write(responseBytes);
          }
        });
  }

  private void attachElectEndpoint() {
    httpServer.createContext(
        "/elect",
        exchange -> {
          var state = runtimeContext.platformState();
          byte[] responseBytes;
          if (!state.isLeader().compareAndSet(false, true)) {
            // failed
            responseBytes =
                String.format("Cannot elect %s, already leader", state.platformType().name())
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, responseBytes.length);
          } else {
            // success
            responseBytes =
                String.format("Elected %s", state.platformType().name())
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
          }

          try (var out = exchange.getResponseBody()) {
            out.write(responseBytes);
          }
        });
  }

  private void attachDemoteEndpoint() {
    httpServer.createContext(
        "/demote",
        exchange -> {
          var state = runtimeContext.platformState();
          byte[] responseBytes;
          if (!state.isLeader().compareAndSet(true, false)) {
            // failed
            responseBytes =
                String.format("Cannot demote %s, not leader", state.platformType().name())
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, responseBytes.length);
          } else {
            // success
            responseBytes =
                String.format("Demoted %s", state.platformType().name())
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
          }

          try (var out = exchange.getResponseBody()) {
            out.write(responseBytes);
          }
        });
  }

  private Response buildHealthResponse() {
    if (runtimeContext.workersToken().isCancelled()) {
      return new Response(500, "down".getBytes(StandardCharsets.UTF_8));
    }
    return new Response(200, "healthy".getBytes(StandardCharsets.UTF_8));
  }

  private record Response(int code, byte[] responseBytes) {}
}
