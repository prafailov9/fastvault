package com.ntros.data;

import com.ntros.IdSequencer;
import com.ntros.data.platform.PlatformType;

public class Message {

  private final int messageId;

  private final Command command;
  private final PlatformType target;

  public Message(Command command, PlatformType target) {
    this.messageId = IdSequencer.getNextMessageId();
    this.command = command;
    this.target = target;
  }

  public int getMessageId() {
    return messageId;
  }

  public Command getCommand() {
    return command;
  }

  public PlatformType getTarget() {
    return target;
  }
}
