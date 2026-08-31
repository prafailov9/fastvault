package com.ntros.workers;

import com.ntros.data.RuntimeContext;
import java.net.http.HttpClient;

public class Uploader implements Runnable {

  private final RuntimeContext runtimeContext;

  public Uploader(RuntimeContext runtimeContext, HttpClient client) {
    this.runtimeContext = runtimeContext;
  }

  @Override
  public void run() {
    while (true) {}
  }
}
