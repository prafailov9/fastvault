package com.ntros.workers;

import static com.ntros.data.Command.ELECT_NEW_LEADER;
import static com.ntros.data.Command.STRIP_LEADERSHIP;
import static com.ntros.data.platform.PlatformType.MAC;
import static com.ntros.data.platform.PlatformType.WINDOWS;

import com.ntros.channel.MessageChannel;
import com.ntros.data.CancellationToken;
import com.ntros.data.Message;
import com.ntros.data.RuntimeContext;
import com.ntros.data.platform.PlatformState;
import java.util.concurrent.BlockingQueue;

public class CliRunner implements Runnable {

  private final PlatformState platformState;
  private final CancellationToken token;
  private final MessageChannel<Message> messageChannel;
  private final BlockingQueue<String> inputCommands;

  public CliRunner(RuntimeContext runtimeContext, BlockingQueue<String> inputCommands) {
    token = runtimeContext.workersToken();
    platformState = runtimeContext.platformState();
    messageChannel = runtimeContext.messageChannel();
    this.inputCommands = inputCommands;
  }

  @Override
  public void run() {
    try {
      while (!token.isCancelled()) {
        displayState();

        String in = inputCommands.take(); // interruptible

        // TODO: finish leader election implementation
        var targetMachine = platformState.platformType() == WINDOWS ? MAC : WINDOWS;
        if (in.equals("r") && platformState.isLeader().get()) {
          platformState.relinquishLeadership();

          messageChannel.put(new Message(ELECT_NEW_LEADER, targetMachine));
        } else if (in.equals("i") && !platformState.isLeader().get()) {
          // request leadership
          messageChannel.put(new Message(STRIP_LEADERSHIP, targetMachine));
          //          platformState.acquireLeadership();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void displayState() {
    System.out.print("\033[H\033[2J");
    System.out.flush();
    String origin = "Origin Machine: " + platformState.platformType().name();
    String target =
        "Target Machine: "
            + (platformState.platformType() == WINDOWS ? MAC.name() : WINDOWS.name());
    String leadership =
        platformState.isLeader().get()
            ? "Leader: yes"
            : "Leader: no";

    System.out.printf("%s | %s | %s%n", origin, leadership, target);

    // footer
    System.out.printf("%n%s%n", buildLeadershipOptions());
  }

  private String buildLeadershipOptions() {
    if (amILeader()) {
      return "Relinquish Leadership: Press 'r'";
    }
    return "Request Leadership: Press 'i'";
  }

  private boolean amILeader() {
    return platformState.isLeader().get();
  }
}
