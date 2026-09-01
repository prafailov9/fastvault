package com.ntros.data.platform;

import com.ntros.data.DeviceAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record PlatformState(
    PlatformType platformType,
    DeviceAddress deviceAddress,
    AtomicBoolean isLeader,
    String homeDir) {

  private static final Logger log = LoggerFactory.getLogger(PlatformState.class);

  public boolean relinquishLeadership() {
    if (isLeader.compareAndSet(true, false)) {
      log.info("Platform {} relinquished leadership.", platformType.name());
      return true;
    }

    log.info("Platform {} is already a follower.", platformType.name());
    return false;
  }

  public boolean acquireLeadership() {
    if (isLeader.compareAndSet(false, true)) {
      log.info("Platform {} acquired leadership.", platformType.name());
      return true;
    }

    log.info("Platform {} is already leader.", platformType.name());
    return false;
  }
}
