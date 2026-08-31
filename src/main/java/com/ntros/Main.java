package com.ntros;

import static com.ntros.data.platform.PlatformType.MAC;
import static com.ntros.data.platform.PlatformType.WINDOWS;

import com.ntros.channel.MessageChannel;
import com.ntros.data.CancellationToken;
import com.ntros.data.DeviceAddress;
import com.ntros.data.Message;
import com.ntros.data.RuntimeContext;
import com.ntros.data.platform.PlatformState;
import com.ntros.runtime.RuntimeController;
import com.ntros.server.HttpServerWrapper;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client/Server file sender for own devices. Both MAC and PC must have the app running to transmit
 * files. By default, PC is the Leader, unless it relinquishes responsibility. Leader means it will
 * be the sole party who uploads/downloads files, the other parties are just listeners to the
 * Leader's requests. Leadership can be attained at any point by the non-leader on request.
 */
public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static final DeviceAddress WIN_DEVICE_ADDRESS = new DeviceAddress("192.168.0.179", 8081);
  private static final DeviceAddress MAC_DEVICE_ADDRESS = new DeviceAddress("192.168.0.87", 8083);

  public static void main(String[] args) {
    // 1. init platform state and runtime context

    // win ip: 192.168.0.179
    // mac ip: 192.168.0.87

    System.out.println("Hello World!");
    String basedir = "vault/shared";
    String ingoingDir = "in";
    String outgoingDir = "out";
    int downloadDelayMs = 250;
    int uploadDelayMs = 250;
    int serverPort = 8081;
    CancellationToken token = new CancellationToken();
    MessageChannel<Message> messageChannel = new MessageChannel<>(1024);
    MessageChannel<Path> fileChannel = new MessageChannel<>(128);

    PlatformState platformState = determinePlatformState();

    RuntimeContext context =
        new RuntimeContext(
            platformState,
            platformState.platformType() == WINDOWS ? MAC_DEVICE_ADDRESS : WIN_DEVICE_ADDRESS,
            basedir,
            ingoingDir,
            outgoingDir,
            downloadDelayMs,
            uploadDelayMs,
            token,
            fileChannel,
            messageChannel);

    // 2. init and start the server.
    HttpServerWrapper serverWrapper = new HttpServerWrapper(serverPort, context);

    // 3. configure client
    HttpClient client = HttpClient.newHttpClient();
    RuntimeController controller = new RuntimeController(context, client);

    shutdown(serverWrapper, controller);
    controller.start();
  }

  private static void shutdown(HttpServerWrapper serverWrapper, RuntimeController controller) {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    serverWrapper.shutdown();
                    controller.stop();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Failed to shutdown gracefully.", e.getCause());
                  }
                },
                "shutdown-hook1"));
  }

  // TODO: read from config
  private static PlatformState determinePlatformState() {
    String os = System.getProperty("os.name").toLowerCase();
    if (os.contains("win")) {
      return new PlatformState(WINDOWS, WIN_DEVICE_ADDRESS, new AtomicBoolean(true), "C:");
    }
    return new PlatformState(MAC, MAC_DEVICE_ADDRESS, new AtomicBoolean(false), "user.home");
  }
}
