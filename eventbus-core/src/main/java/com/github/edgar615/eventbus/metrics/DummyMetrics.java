package com.github.edgar615.eventbus.metrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class DummyMetrics implements ProducerMetrics, ConsumerMetrics {
  @Override
  public void sendEnqueue() {

  }

  @Override
  public void consumerStart() {

  }

  @Override
  public void consumerEnd(long duration) {

  }

  @Override
  public void sendStart() {

  }

  @Override
  public void sendEnd(boolean result, long duration) {

  }

  @Override
  public Map<String, Object> metrics() {
    return new HashMap<>();
  }
}
