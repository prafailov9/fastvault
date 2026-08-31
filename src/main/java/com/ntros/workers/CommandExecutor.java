package com.ntros.workers;

import com.ntros.channel.MessageChannel;
import com.ntros.data.CancellationToken;
import com.ntros.data.DeviceAddress;
import com.ntros.data.Message;
import com.ntros.data.RuntimeContext;
import com.ntros.data.platform.PlatformState;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandExecutor implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(CommandExecutor.class);
  private final MessageChannel<Message> messageChannel;
  private final CancellationToken token;
  private final PlatformState platformState;
  private final HttpClient client;
  private final DeviceAddress targetDeviceAddress;
  private final String baseUri;

  public CommandExecutor(RuntimeContext runtimeContext, HttpClient client) {
    messageChannel = runtimeContext.messageChannel();
    token = runtimeContext.workersToken();
    platformState = runtimeContext.platformState();
    this.client = client;
    targetDeviceAddress = runtimeContext.targetDeviceAddress();
    baseUri = String.format("http://%s:%s", targetDeviceAddress.host(), targetDeviceAddress.port());
  }

  @Override
  public void run() {
    while (!token.isCancelled()) {
      Message msg = null;
      try {
        msg = messageChannel.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (msg == null) {
        continue;
      }
      log.info("Received msg: {}", msg);
      var cmd = msg.getCommand();
      switch (cmd) {
        case ELECT_NEW_LEADER -> {
          // this machine already relinquished leadership
          // tell target that he is the new leader
          if (!send("elect")) {
            log.info("failed to elect target. Re-acquiring leadership");
            platformState.acquireLeadership();
          }
        }
        case STRIP_LEADERSHIP -> {
          // this machine wants leadership
          // tell target he is no longer the leader
          if (!send("demote")) {
            log.info("failed to demote target");
          }
        }
        default -> {}
      }
    }
  }

  private boolean send(String resource) {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(String.format("%s/%s", baseUri, resource)))
            .GET()
            .build();
    HttpResponse<String> response;
    try {
      response = client.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        log.info("Response: {}", response.body());
        return false;
      }
      return true;
    } catch (IOException | InterruptedException e) {
      return false;
    }
  }
}
