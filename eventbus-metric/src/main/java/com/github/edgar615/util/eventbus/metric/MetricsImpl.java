package com.github.edgar615.util.eventbus.metric;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.edgar615.util.eventbus.Metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
class MetricsImpl implements Metrics {
  private final MetricRegistry registry;

  private final Counter consumerCompleted;

  private final Counter consumerProcessed;

  private final Timer consumerTimer;

  private final Counter sendPending;

  private final Counter sendSucceed;

  private final Counter sendFailed;

  private final Counter sendProcessed;

  private final Timer sendTimer;

  public static Metrics create(String baseName) {
    MetricRegistry metrics = new MetricRegistry();
    return new MetricsImpl(metrics, baseName);
  }

  MetricsImpl(MetricRegistry registry, String baseName) {
    this.registry = registry;
    this.sendPending = registry.counter(MetricRegistry.name(baseName, "send", "pending"));
    this.sendSucceed = registry.counter(MetricRegistry.name(baseName, "send", "succeed"));
    this.sendFailed = registry.counter(MetricRegistry.name(baseName, "send", "failed"));
    this.sendProcessed = registry.counter(MetricRegistry.name(baseName, "send", "processed"));
    this.sendTimer = registry.timer(MetricRegistry.name(baseName, "send"));

    this.consumerCompleted = registry.counter(MetricRegistry.name(baseName, "consumer",
                                                                  "completed"));
    this.consumerProcessed =
            registry.counter(MetricRegistry.name(baseName, "consumer", "processed"));
    this.consumerTimer = registry.timer(MetricRegistry.name(baseName, "consumer"));
  }

  @Override
  public void sendEnqueue() {
    sendPending.inc();
  }

  @Override
  public void consumerStart() {
    consumerProcessed.inc();
  }

  @Override
  public void consumerEnd(long duration) {
    consumerProcessed.dec();
    consumerCompleted.inc();
    consumerTimer.update(duration, TimeUnit.SECONDS);
  }

  @Override
  public void sendStart() {
    sendProcessed.inc();
    sendPending.dec();
  }

  @Override
  public void sendEnd(boolean result, long duration) {
    if (result) {
      sendSucceed.inc();
    } else {
      sendFailed.inc();
    }
    sendProcessed.dec();
    sendTimer.update(duration, TimeUnit.SECONDS);
  }

  @Override
  public Map<String, Object> metrics() {
    Map<String, Object> map = registry.getMetrics().
            entrySet().
            stream().
            collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> Helper.convertMetric(e.getValue())));
    return new HashMap<>(map);
  }
}
