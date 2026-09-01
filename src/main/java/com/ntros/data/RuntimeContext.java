package com.ntros.data;

import com.ntros.channel.MessageChannel;
import com.ntros.data.platform.PlatformState;

public record RuntimeContext(
    PlatformState platformState,
    DeviceAddress targetDeviceAddress,
    String basedir,
    String ingoing,
    String outgoing,
    String archive,
    int dwDelayMs,
    int upDelayMs,
    int serverWorkers,
    int dwPermits,
    CancellationToken workersToken,
    MessageChannel<Message> messageChannel) {}
