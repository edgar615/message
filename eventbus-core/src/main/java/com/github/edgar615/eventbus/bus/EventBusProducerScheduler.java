package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.dao.EventProducerDao;

public interface EventBusProducerScheduler {

  static EventBusProducerScheduler create(EventProducerDao eventProducerDao,
      EventBusWriteStream writeStream, long fetchPeriod) {
    return new EventBusProducerSchedulerImpl(eventProducerDao, writeStream, fetchPeriod);
  }

  void start();

  void close();

}
