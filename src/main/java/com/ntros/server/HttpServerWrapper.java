package com.ntros.server;

import com.ntros.Shutdownable;
import com.ntros.data.RuntimeContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServerWrapper implements Server, Shutdownable {
  private static final int WORKERS_POOL_LEN = 5;
  private static final Logger log = LoggerFactory.getLogger(HttpServerWrapper.class);
  private final HttpServer httpServer;
  private final RuntimeContext runtimeContext;

  public HttpServerWrapper(RuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
    int port = runtimeContext.platformState().deviceAddress().port();
    httpServer = createServer(port);
    // server delegates requests to workers. Unblocks VTs downloading on the Leader side.
    httpServer.setExecutor(Executors.newFixedThreadPool(WORKERS_POOL_LEN));
    attachHealthEndpoint();
    attachLeadershipEndpoint();
    attachGetFilesEndpoint();
    attachDownloadEndpoint();
    attachCleanupEndpoint();
    attachElectEndpoint();
    attachDemoteEndpoint();
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

  private void attachLeadershipEndpoint() {
    httpServer.createContext(
        "/leadership",
        exchange -> {
          Response res = buildLeadershipResponse();
          var bytes = res.responseBytes;
          exchange.sendResponseHeaders(res.code, bytes.length);

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
            log.info("Listing files");
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
          String name = queryParams(exchange).get("filename");
          if (name == null
              || name.isBlank()
              || name.contains("/")
              || name.contains("\\")
              || name.contains("..")) {
            respond(exchange, 400, "bad filename");
            return;
          }
          Path outDir =
              Paths.get(
                  runtimeContext.platformState().homeDir(),
                  runtimeContext.basedir(),
                  runtimeContext.outgoing());
          Path file = outDir.resolve(name);
          if (!Files.isRegularFile(file)) {
            respond(exchange, 404, "not found: " + name);
            return;
          }
          exchange.sendResponseHeaders(200, Files.size(file)); // the promise
          try (var out = exchange.getResponseBody()) {
            Files.copy(file, out); // keeping it
          }
        });
  }

  private void attachCleanupEndpoint() {
    httpServer.createContext(
        "/ack",
        exchange -> {
          String name = queryParams(exchange).get("filename");
          if (name == null
              || name.isBlank()
              || name.contains("/")
              || name.contains("\\")
              || name.contains("..")) {
            respond(exchange, 400, "bad filename");
            return;
          }
          Path outDir =
              Paths.get(
                  runtimeContext.platformState().homeDir(),
                  runtimeContext.basedir(),
                  runtimeContext.outgoing());

          Path sentDir =
              Paths.get(runtimeContext.platformState().homeDir(), runtimeContext.basedir(), "sent");

          Path src = outDir.resolve(name);
          Path dest = sentDir.resolve(name);
          if (!Files.isRegularFile(src)) {
            respond(exchange, 404, "not found: " + name);
            return;
          }
          Files.move(src, dest, StandardCopyOption.ATOMIC_MOVE);
          byte[] res = "file cleared".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, res.length); // the promise
          try (var out = exchange.getResponseBody()) {
            out.write(res);
          }
        });
  }

  private static void respond(HttpExchange ex, int code, String text) throws IOException {
    byte[] body = text.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(code, body.length);
    try (var out = ex.getResponseBody()) {
      out.write(body);
    }
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

  private Response buildLeadershipResponse() {
    var state = runtimeContext.platformState();
    if (state == null || state.isLeader() == null) {
      return new Response(500, "UNKNOWN".getBytes(StandardCharsets.UTF_8));
    }
    int status = 200;
    if (state.isLeader().get()) {
      return new Response(status, "LEADER".getBytes(StandardCharsets.UTF_8));
    }
    return new Response(status, "FOLLOWER".getBytes(StandardCharsets.UTF_8));
  }

  private Response buildHealthResponse() {
    if (runtimeContext.workersToken().isCancelled()) {
      return new Response(500, "down".getBytes(StandardCharsets.UTF_8));
    }
    return new Response(200, "healthy".getBytes(StandardCharsets.UTF_8));
  }

  private static Map<String, String> queryParams(HttpExchange ex) {
    Map<String, String> m = new HashMap<>();
    String raw = ex.getRequestURI().getRawQuery(); // raw, not getQuery()
    if (raw == null) return m;
    for (String pair : raw.split("&")) {
      int i = pair.indexOf('=');
      if (i > 0)
        m.put(
            URLDecoder.decode(pair.substring(0, i), StandardCharsets.UTF_8),
            URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8));
    }
    return m;
  }

  private record Response(int code, byte[] responseBytes) {}
}
