package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.bus.EventBusWriteStream;
import com.github.edgar615.eventbus.repository.EventProducerRepository;
import io.vertx.core.Vertx;

public interface VertxEventBusProducerScheduler {

  static VertxEventBusProducerScheduler create(Vertx vertx, VertxEventProducerRepository eventProducerRepository,
      VertxEventBusWriteStream writeStream, long fetchPeriod) {
    return new VertxEventBusProducerSchedulerImpl(vertx, eventProducerRepository, writeStream, fetchPeriod);
  }

  void start();

  void close();

}
