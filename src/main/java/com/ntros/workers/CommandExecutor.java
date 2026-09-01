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
      /**
       *
       *
       * <pre>
       * Test flows:
       *  1. Start both → Windows leader, Mac follower.
       *  2. Windows presses r → Windows follower, Mac leader.
       *  3. Mac presses r → Mac follower, Windows leader.
       *  4. Mac presses i while Windows leads → Windows follower, Mac leader.
       *  5. Windows presses i while Mac leads → Mac follower, Windows leader.
       * Kill the target machine during a handoff and observe what happens.
       * </pre>
       */
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
            platformState.acquireLeadership();
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

  // private SendResult send(String resource) {
  //  HttpRequest request =
  //      HttpRequest.newBuilder()
  //          .uri(URI.create("%s/%s".formatted(baseUri, resource)))
  //          .POST(HttpRequest.BodyPublishers.noBody())
  //          .build();
  //
  //  try {
  //    var response =
  //        client.send(
  //            request,
  //            HttpResponse.BodyHandlers.ofString());
  //
  //    if (response.statusCode() == 200) {
  //      return SendResult.SUCCESS;
  //    }
  //
  //    log.info("Response: {}", response.body());
  //    return SendResult.REJECTED;
  //
  //  } catch (IOException e) {
  //    log.warn("Could not reach target", e);
  //    return SendResult.UNREACHABLE;
  //
  //  } catch (InterruptedException e) {
  //    Thread.currentThread().interrupt();
  //    return SendResult.INTERRUPTED;
  //  }
  // }
  //
  // private enum SendResult {
  //  SUCCESS,
  //  REJECTED,
  //  UNREACHABLE,
  //  INTERRUPTED
  // }

}
