package com.ntros.data;

import com.ntros.channel.MessageChannel;
import com.ntros.data.platform.PlatformState;
import java.nio.file.Path;

public record RuntimeContext(
    PlatformState platformState,
    DeviceAddress targetDeviceAddress,
    String basedir,
    String ingoing,
    String outgoing,
    int dwDelayMs,
    int upDelayMs,
    CancellationToken workersToken,
    MessageChannel<Path> fileChannel,
    MessageChannel<Message> messageChannel) {}
