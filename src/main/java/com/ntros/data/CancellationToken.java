package com.ntros.data;

import java.util.concurrent.atomic.AtomicBoolean;

public class CancellationToken {

  private final AtomicBoolean token = new AtomicBoolean(false);


  public boolean isCancelled() {
    return token.get();
  }

  public void cancel() {
    token.set(true);
  }
}
