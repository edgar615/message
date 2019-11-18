package com.github.edgar615.message.vertx;

import io.vertx.core.Vertx;

public interface VertxMessageProducerScheduler {

  static VertxMessageProducerScheduler create(Vertx vertx, VertxMessageProducerRepository eventProducerRepository,
      VertxMessageWriteStream writeStream, long fetchPeriod) {
    return new VertxMessageProducerSchedulerImpl(vertx, eventProducerRepository, writeStream, fetchPeriod);
  }

  void start();

  void close();

}
