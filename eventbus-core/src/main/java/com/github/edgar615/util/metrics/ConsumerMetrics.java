package com.github.edgar615.util.metrics;

/**
 * 度量指标.
 *
 * @author Edgar  Date 2017/3/29
 */
public interface ConsumerMetrics extends Metrics {

  void consumerStart();

  void consumerEnd(long duration);

}
