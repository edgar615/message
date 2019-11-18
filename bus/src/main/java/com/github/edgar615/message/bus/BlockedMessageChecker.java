package com.github.edgar615.message.bus;

import com.github.edgar615.message.utils.LoggingMarker;
import com.github.edgar615.message.utils.NamedThreadFactory;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockedMessageChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockedMessageChecker.class);

  private static final Object O = new Object();

  private final Map<BlockedMessageHolder, Object> map = new WeakHashMap<>();

  private final ScheduledExecutorService scheduledExecutorService;
  private BlockedMessageChecker(long interval, ScheduledExecutorService scheduledExecutorService) {
    this.scheduledExecutorService = scheduledExecutorService;
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      synchronized (BlockedMessageChecker.this) {
        map.keySet()
            .removeIf(r -> r.isCompleted());
        List<BlockedMessageHolder> holders =
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

  public synchronized void register(BlockedMessageHolder holder) {
    map.put(holder, O);
  }

  public void close() {
    this.scheduledExecutorService.shutdown();
  }

  public static BlockedMessageChecker create(long interval) {
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory.create("blocked-checker"));
    return new BlockedMessageChecker(interval, executorService);
  }

}
