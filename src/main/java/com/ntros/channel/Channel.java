package com.ntros.channel;

public interface Channel<E> {

  boolean put(E value);

  E take() throws InterruptedException;

  boolean isEmpty();

  int size();
}
