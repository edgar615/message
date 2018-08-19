package com.github.edgar615.util.eventbus.metric;

import com.codahale.metrics.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Edgar on 2017/3/24.
 *
 * @author Edgar  Date 2017/3/24
 */
public class Helper {

  public static Map<String, Object> convertMetric(Metric metric) {
    if (metric instanceof Timer) {
      return toJson((Timer) metric);
    } else if (metric instanceof Gauge) {
      return toJson((Gauge) metric);
    } else if (metric instanceof Counter) {
      return toJson((Counter) metric);
    } else if (metric instanceof Histogram) {
      return toJson((Histogram) metric);
    } else if (metric instanceof Meter) {
      return toJson((Meter) metric);
    } else {
      throw new IllegalArgumentException("Unknown metric " + metric);
    }
  }

  private static Map<String, Object> toJson(Meter meter) {
    Map<String, Object> map = new HashMap<>();
    map.put("type", "meter");
    map.put("count", meter.getCount());
    map.put("meanRate", meter.getMeanRate());
    map.put("oneMinuteRate", meter.getOneMinuteRate());
    map.put("fiveMinuteRate", meter.getFiveMinuteRate());
    map.put("fifteenMinuteRate", meter.getFifteenMinuteRate());
    map.put("rate", "events/second");
    return map;
  }

  private static Map<String, Object> toJson(Histogram histogram) {
    Snapshot snapshot = histogram.getSnapshot();
    Map<String, Object> map = new HashMap<>();
    map.put("type", "histogram");
    map.put("count", histogram.getCount());
    map.put("min", snapshot.getMin());
    map.put("max", snapshot.getMax());
    map.put("mean", snapshot.getMean());
    map.put("stddev", snapshot.getStdDev());
    map.put("median", snapshot.getMedian());
    map.put("75%", snapshot.get75thPercentile());
    map.put("95%", snapshot.get95thPercentile());
    map.put("98%", snapshot.get98thPercentile());
    map.put("99%", snapshot.get99thPercentile());
    map.put("99.9%", snapshot.get999thPercentile());
    return map;
  }

  private static Map<String, Object> toJson(Gauge gauge) {
    Map<String, Object> map = new HashMap<>();
    map.put("type", "gauge");
    map.put("value", gauge.getValue());
    return map;
  }

  private static Map<String, Object> toJson(Timer timer) {
    Map<String, Object> map = new HashMap<>();
    map.put("type", "timer");
    map.put("count", timer.getCount());
    map.put("meanRate", timer.getMeanRate());
    map.put("oneMinuteRate", timer.getOneMinuteRate());
    map.put("fiveMinuteRate", timer.getFiveMinuteRate());
    map.put("fifteenMinuteRate", timer.getFifteenMinuteRate());
    map.put("rate", "events/second");

    Snapshot snapshot = timer.getSnapshot();
    map.put("min", snapshot.getMin());
    map.put("max", snapshot.getMax());
    map.put("mean", snapshot.getMean());
    map.put("stddev", snapshot.getStdDev());
    map.put("median", snapshot.getMedian());
    map.put("75%", snapshot.get75thPercentile());
    map.put("95%", snapshot.get95thPercentile());
    map.put("98%", snapshot.get98thPercentile());
    map.put("99%", snapshot.get99thPercentile());
    map.put("99.9%", snapshot.get999thPercentile());
    return map;
  }

  private static Map<String, Object> toJson(Counter counter) {
    Map<String, Object> map = new HashMap<>();
    map.put("type", "counter");
    map.put("count", counter.getCount());
    return map;
  }

}
