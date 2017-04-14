package com.edgar.util.eventbus;

import com.edgar.util.eventbus.metric.Metrics;

/**
 * 消息发送的接口.
 *
 * @author Edgar  Date 2017/3/24
 */
public interface ConsumerBackend extends Runnable {

  static ConsumerBackend create(
          ConsumerOptions options, Metrics metrics) {
    return new ConsumerBackendImpl(options, metrics);
  }
}
