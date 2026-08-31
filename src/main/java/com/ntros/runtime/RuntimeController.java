package com.ntros.runtime;

import com.ntros.data.CancellationToken;
import com.ntros.data.RuntimeContext;
import com.ntros.data.platform.PlatformState;
import com.ntros.workers.CliRunner;
import com.ntros.workers.CommandExecutor;
import com.ntros.workers.ConsoleInputReader;
import com.ntros.workers.Downloader;
import com.ntros.workers.FileSaver;
import com.ntros.workers.Uploader;
import java.net.http.HttpClient;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * handles the lifecycle when this app is the Leader. By default, PC is the Leader, so it will
 * immediately run the Controller. On MAC, The controller needs to wait until the PC has explicitly
 * relinquished Leadership.
 */
public class RuntimeController implements Runtime {

  private static final Logger log = LoggerFactory.getLogger(RuntimeController.class);
  // pulls from external source, writes locally
  private final Thread downloader;
  // pulls from local source, writes to external target
  private final Thread uploader;
  private final Thread saver;
  private final Thread cli;
  private final Thread commandExecutor;
  private final CancellationToken token;
  private final PlatformState platformState;

  public RuntimeController(RuntimeContext context, HttpClient client) {
    token = context.workersToken();
    platformState = context.platformState();
    downloader = new Thread(new Downloader(context, client), "ingress-1");
    uploader = new Thread(new Uploader(context, client), "egress-1");
    saver = new Thread(new FileSaver(context), "saver-1");

    BlockingQueue<String> inputCommands = new LinkedBlockingQueue<>();

    cli = new Thread(new CliRunner(context, inputCommands), "cli-1");
    commandExecutor = new Thread(new CommandExecutor(context, client), "cmd-executor-1");
    Thread input = new Thread(new ConsoleInputReader(inputCommands), "input-scanner-1");
    input.setDaemon(true);
    input.start();
  }

  // TODO: figure out how to re-acquire leadership
  @Override
  public void start() {
    downloader.start();
    //    saver.start();
    cli.start();
    commandExecutor.start();
  }

  @Override
  public void stop() throws InterruptedException {
    token.cancel();
    downloader.interrupt();
    downloader.join();

    //    saver.interrupt();
    //    saver.join();

    cli.interrupt();
    cli.join();

    commandExecutor.interrupt();
    commandExecutor.join();
    log.info("Controller shutdown");
    //    egress.join();
  }
}
