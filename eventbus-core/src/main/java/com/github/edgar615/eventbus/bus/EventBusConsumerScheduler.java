package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.repository.EventConsumerRepository;
import com.github.edgar615.eventbus.utils.EventQueue;

public interface EventBusConsumerScheduler {

  static EventBusConsumerScheduler create(EventConsumerRepository eventConsumerRepository,
      EventQueue queue, long fetchPeriod) {
    return new EventBusConsumerSchedulerImpl(eventConsumerRepository, queue, fetchPeriod);
  }

  void start();

  void close();

}
