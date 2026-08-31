package com.ntros.workers;

import java.util.Scanner;
import java.util.concurrent.BlockingQueue;

public class ConsoleInputReader implements Runnable {

  private final BlockingQueue<String> inputCommands;

  public ConsoleInputReader(BlockingQueue<String> inputCommands) {
    this.inputCommands = inputCommands;
  }

  @Override
  public void run() {
    try (Scanner scanner = new Scanner(System.in)) {
      while (scanner.hasNext()) {
        inputCommands.put(scanner.next());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
