package com.ntros.workers;

import com.ntros.channel.MessageChannel;
import com.ntros.data.CancellationToken;
import com.ntros.data.RuntimeContext;
import com.ntros.data.platform.PlatformState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSaver implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(FileSaver.class);
  private final MessageChannel<Path> fileChannel;
  private final CancellationToken token;
  private final RuntimeContext runtimeContext;
  private final PlatformState platformState;

  public FileSaver(RuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
    platformState = runtimeContext.platformState();
    token = runtimeContext.workersToken();
    fileChannel = runtimeContext.fileChannel();
  }

  @Override
  public void run() {
    Path target =
        Paths.get(platformState.homeDir(), runtimeContext.basedir(), runtimeContext.ingoing());
    while (!token.isCancelled()) {
      Path file;
      try {
        file = fileChannel.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.info("Interrupted while waiting");
        continue;
      }
      if (file == null) {
        continue;
      }
      try {
        // TODO: figure out how to create a file at target dir
        Files.createFile(file);
      } catch (IOException e) {
        log.error("Could not create file {}", file.getFileName(), e);
      }
    }
  }
}
