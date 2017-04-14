package com.edgar.util.eventbus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BlockedEventChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockedEventChecker.class);

  private static final Object O = new Object();

  private final Map<RecordMeta, Object> map = new WeakHashMap<>();

  private BlockedEventChecker(long interval, long maxExecTime,
                              ScheduledExecutorService scheduledExecutorService) {
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      synchronized (BlockedEventChecker.this) {
        map.keySet()
                .removeIf(r -> r.isCompleted());
        map.keySet().stream()
                .filter(r -> !r.isCompleted())
                .filter(r -> r.duration() > maxExecTime)
                .forEach(r -> {
                  LOGGER.warn("---| [{}] [blocked {}ms]",
                              r.event().head().id(), r.duration());
                });
      }
    }, interval, interval, TimeUnit.MILLISECONDS);

  }

  public synchronized void register(RecordMeta recordMeta) {
    map.put(recordMeta, O);
  }

  public static BlockedEventChecker create(long interval, long maxExecTime,
                                           ScheduledExecutorService scheduledExecutorService) {
    return new BlockedEventChecker(interval, maxExecTime, scheduledExecutorService);
  }

}