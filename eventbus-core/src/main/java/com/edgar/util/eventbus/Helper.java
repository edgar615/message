package com.edgar.util.eventbus;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.edgar.util.event.Event;
import com.edgar.util.event.EventAction;
import com.edgar.util.event.EventHead;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/24.
 *
 * @author Edgar  Date 2017/3/24
 */
public class Helper {

  public static String toHeadString(Event event) {
    EventHead head = event.head();
    StringBuilder s = new StringBuilder();
    s.append("id:").append(head.id()).append(";")
            .append("to:").append(head.to()).append(";")
            .append("action:").append(head.action()).append(";")
            .append("timestamp:").append(head.timestamp()).append(";")
            .append("duration:").append(head.duration()).append(";");
    head.ext().forEach((k, v) -> s.append(k).append(":").append(v).append(";"));

    return s.toString();
  }

  public static String toActionString(Event event) {
    EventAction action = event.action();
    List<String> actions = Event.codecList.stream()
            .filter(c -> action.name().equalsIgnoreCase(c.name()))
            .map(c -> c.encode(action))
            .map(m -> {
              StringBuilder s = new StringBuilder();
              m.forEach((k, v) -> s.append(k + ":" + v + ";"));
              return s.toString();
            })
            .collect(Collectors.toList());
    return actions.get(0);
  }

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
