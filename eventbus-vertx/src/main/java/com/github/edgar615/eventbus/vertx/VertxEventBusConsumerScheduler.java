package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.utils.EventQueue;
import io.vertx.core.Vertx;

public interface VertxEventBusConsumerScheduler {

  static VertxEventBusConsumerScheduler create(Vertx vertx,
      VertxEventConsumerRepository eventConsumerRepository,
      EventQueue queue, long fetchPeriod) {
    return new VertxEventBusConsumerSchedulerImpl(vertx, eventConsumerRepository, queue, fetchPeriod);
  }

  void start();

  void close();

}
