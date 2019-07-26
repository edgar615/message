package com.github.edgar615.eventbus.vertx;

import io.vertx.core.Vertx;

public interface VertxEventBusProducerScheduler {

  static VertxEventBusProducerScheduler create(Vertx vertx, VertxEventProducerRepository eventProducerRepository,
      VertxEventBusWriteStream writeStream, long fetchPeriod) {
    return new VertxEventBusProducerSchedulerImpl(vertx, eventProducerRepository, writeStream, fetchPeriod);
  }

  void start();

  void close();

}
