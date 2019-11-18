package com.github.edgar615.message.bus;

import com.github.edgar615.message.repository.MessageConsumerRepository;
import com.github.edgar615.message.utils.MessageQueue;

public interface MessageConsumerScheduler {

  static MessageConsumerScheduler create(MessageConsumerRepository messageConsumerRepository,
      MessageQueue queue, long fetchPeriod) {
    return new MessageConsumerSchedulerImpl(messageConsumerRepository, queue, fetchPeriod);
  }

  void start();

  void close();

}
