package com.edgar.util.eventbus;

import java.util.Map;

/**
 * 度量指标.
 *
 * @author Edgar  Date 2017/3/29
 */
public interface Metrics {
  void sendEnqueue();

  void consumerStart();

  void consumerEnd(long duration);

  void sendStart();

  void sendEnd(boolean result, long duration);

  Map<String, Object> metrics();

}
