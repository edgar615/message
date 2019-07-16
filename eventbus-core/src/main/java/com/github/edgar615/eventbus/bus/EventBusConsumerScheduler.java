package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.dao.EventConsumerDao;
import com.github.edgar615.eventbus.utils.EventQueue;

public interface EventBusConsumerScheduler {

  static EventBusConsumerScheduler create(EventConsumerDao eventConsumerDao,
      EventQueue queue, long fetchPeriod) {
    return new EventBusConsumerSchedulerImpl(eventConsumerDao, queue, fetchPeriod);
  }

  void start();

  void close();

}
