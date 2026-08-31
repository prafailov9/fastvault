package com.ntros.channel;

import java.util.ArrayDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageChannel<E> implements Channel<E> {

  private static final Logger log = LoggerFactory.getLogger(MessageChannel.class);
  private final ArrayDeque<E> queue = new ArrayDeque<>();
  private final Object lock = new Object();
  private final int capacity;

  public MessageChannel(int capacity) {
    this.capacity = capacity;
  }

  @Override
  public boolean offer(E value) {
    synchronized (lock) {
      while (queue.size() >= capacity) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.error("Interrupted while waiting");
          return false;
        }
      }
      queue.add(value);
      lock.notifyAll();
      return true;
    }
  }

  @Override
  public E take() throws InterruptedException {
    synchronized (lock) {
      while (queue.isEmpty()) {
        lock.wait();
      }
      E value = queue.remove();
      lock.notifyAll();
      return value;
    }
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public int size() {
    return queue.size();
  }
}
