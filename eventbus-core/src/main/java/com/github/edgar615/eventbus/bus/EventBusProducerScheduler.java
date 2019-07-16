package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.repository.EventProducerRepository;

public interface EventBusProducerScheduler {

  static EventBusProducerScheduler create(EventProducerRepository eventProducerRepository,
      EventBusWriteStream writeStream, long fetchPeriod) {
    return new EventBusProducerSchedulerImpl(eventProducerRepository, writeStream, fetchPeriod);
  }

  void start();

  void close();

}
