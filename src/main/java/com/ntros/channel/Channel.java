package com.ntros.channel;

public interface Channel<E> {

  boolean offer(E value);

  E take() throws InterruptedException;

  boolean isEmpty();

  int size();
}
