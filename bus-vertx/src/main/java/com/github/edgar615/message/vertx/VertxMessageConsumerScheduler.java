package com.github.edgar615.message.vertx;

import com.github.edgar615.message.utils.MessageQueue;
import io.vertx.core.Vertx;

public interface VertxMessageConsumerScheduler {

  static VertxMessageConsumerScheduler create(Vertx vertx,
      VertxMessageConsumerRepository eventConsumerRepository,
      MessageQueue queue, long fetchPeriod) {
    return new VertxMessageConsumerSchedulerImpl(vertx, eventConsumerRepository, queue, fetchPeriod);
  }

  void start();

  void close();

}
