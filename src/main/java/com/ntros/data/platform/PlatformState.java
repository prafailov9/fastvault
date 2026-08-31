package com.ntros.data.platform;

import com.ntros.data.DeviceAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record PlatformState(
    PlatformType platformType, DeviceAddress deviceAddress, AtomicBoolean isLeader, String homeDir) {

  private static final Logger log = LoggerFactory.getLogger(PlatformState.class);

  public void relinquishLeadership() {
    if (isLeader.compareAndSet(true, false)) {
      log.info("Platform {} no longer leader. Notifying systems...", platformType.name());
    }
    log.info("Current platform {} was not the leader.", platformType.name());
  }

  public void acquireLeadership() {
    if (isLeader.compareAndSet(false, true)) {
      log.info("Platform {} is now leader. Notifying systems...", platformType.name());
    }
    log.info("Current platform {} already the leader.", platformType.name());
  }
}
