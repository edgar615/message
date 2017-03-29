package com.edgar.util.metirc;

import com.codahale.metrics.MetricRegistry;

import java.util.Map;

/**
 * 度量指标.
 *
 * @author Edgar  Date 2017/3/29
 */
public interface Metrics {
  void sendEnqueue();

  void sendStart();

  void sendEnd(boolean result, long duration);

  Map<String, Object> metrics();

  static Metrics create(String baseName) {
    MetricRegistry metrics = new MetricRegistry();
    return new MetricsImpl(metrics, baseName);
  }
}
