package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class BlockedEventChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockedEventChecker.class);

  private static final Object O = new Object();

  private final Map<BlockedEventHolder, Object> map = new WeakHashMap<>();

  private BlockedEventChecker(long interval,
                              ScheduledExecutorService scheduledExecutorService) {
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      synchronized (BlockedEventChecker.this) {
        map.keySet()
                .removeIf(r -> r.isCompleted());
        map.keySet().stream()
                .filter(r -> !r.isCompleted())
                .filter(r -> r.duration() > r.maxExecTime())
                .forEach(r -> Log.create(LOGGER)
                        .setLogType("eventbus")
                        .setTraceId(r.event().head().id())
                        .setEvent("event.blocked")
                        .addData("duration", r.duration())
                        .warn());
      }
    }, interval, interval, TimeUnit.MILLISECONDS);

  }

  public synchronized void register(BlockedEventHolder holder) {
    map.put(holder, O);
  }

  static BlockedEventChecker create(long interval,
                                    ScheduledExecutorService scheduledExecutorService) {
    return new BlockedEventChecker(interval, scheduledExecutorService);
  }

}