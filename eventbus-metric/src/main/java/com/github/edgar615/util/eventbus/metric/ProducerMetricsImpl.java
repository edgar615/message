package com.github.edgar615.util.eventbus.metric;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.github.edgar615.util.metrics.ProducerMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class ProducerMetricsImpl implements ProducerMetrics {
  private final MetricRegistry registry;

  private final Counter sendPending;

  private final Counter sendSucceed;

  private final Counter sendFailed;

  private final Counter sendProcessed;

  private final Timer sendTimer;

  public ProducerMetricsImpl() {
    this.registry = SharedMetricRegistries.getOrCreate("eventbus");
    String baseName = "eventbus";
    this.sendPending = registry.counter(MetricRegistry.name(baseName, "send", "pending"));
    this.sendSucceed = registry.counter(MetricRegistry.name(baseName, "send", "succeed"));
    this.sendFailed = registry.counter(MetricRegistry.name(baseName, "send", "failed"));
    this.sendProcessed = registry.counter(MetricRegistry.name(baseName, "send", "processed"));
    this.sendTimer = registry.timer(MetricRegistry.name(baseName, "send"));
  }

  @Override
  public void sendEnqueue() {
    sendPending.inc();
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
