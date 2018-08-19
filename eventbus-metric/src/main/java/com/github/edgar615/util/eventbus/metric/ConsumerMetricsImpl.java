package com.github.edgar615.util.eventbus.metric;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.edgar615.util.metrics.ConsumerMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class ConsumerMetricsImpl implements ConsumerMetrics {
  private final MetricRegistry registry;

  private final Counter consumerCompleted;

  private final Counter consumerProcessed;

  private final Timer consumerTimer;

  public ConsumerMetricsImpl() {
    this.registry = new MetricRegistry();
    String baseName = "eventbus";
    this.consumerCompleted = registry.counter(MetricRegistry.name(baseName, "consumer",
            "completed"));
    this.consumerProcessed =
            registry.counter(MetricRegistry.name(baseName, "consumer", "processed"));
    this.consumerTimer = registry.timer(MetricRegistry.name(baseName, "consumer"));
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
