package com.github.edgar615.message.bus;

import com.github.edgar615.message.repository.MessageProducerRepository;

public interface MessageProducerScheduler {

  static MessageProducerScheduler create(MessageProducerRepository messageProducerRepository,
      MessageWriteStream writeStream, long fetchPeriod) {
    return new MessageProducerSchedulerImpl(messageProducerRepository, writeStream, fetchPeriod);
  }

  void start();

  void close();

}
