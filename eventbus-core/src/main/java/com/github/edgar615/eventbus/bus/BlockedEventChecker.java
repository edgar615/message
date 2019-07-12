package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.utils.LoggingMarker;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
          LOGGER.warn("blocked {} events", holders.size());
        }

        holders.forEach(r -> LOGGER.warn(
            LoggingMarker.getLoggingMarker(r.eventId(), ImmutableMap.of("duration", r.duration())),
            "blocked {}ms", r.duration()));
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
