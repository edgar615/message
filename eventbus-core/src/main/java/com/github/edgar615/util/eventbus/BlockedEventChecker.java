package com.github.edgar615.util.eventbus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
        List<BlockedEventHolder> holders =
                map.keySet().stream()
                        .filter(r -> !r.isCompleted())
                        .filter(r -> r.duration() > r.maxExecTime())
                        .collect(Collectors.toList());
        if (!holders.isEmpty()) {
          LOGGER.warn("[EC] [BLOCKED] [{}]", holders.size());
        }

        holders.forEach(r -> LOGGER.warn("[{}] [EC] [BLOCKED] [{}ms]", r.eventId(), r.duration()));
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