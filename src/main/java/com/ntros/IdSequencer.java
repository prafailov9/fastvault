package com.ntros;

import java.util.concurrent.atomic.AtomicInteger;

public class IdSequencer {

  private static final AtomicInteger NEXT_MESSAGE_ID = new AtomicInteger(0);

  public static int getNextMessageId() {
    return NEXT_MESSAGE_ID.incrementAndGet();
  }
}
