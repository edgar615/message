package com.edgar.util.eventbus.metric;

import java.util.Map;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class DummyMetrics implements Metrics {
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
    return null;
  }
}
