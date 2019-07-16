package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.repository.EventConsumerRepository;
import com.github.edgar615.eventbus.utils.EventQueue;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
public interface EventBusConsumer {

  static EventBusConsumer create(ConsumerOptions options, EventQueue queue) {
    return new EventBusConsumerImpl(options, queue, null);
  }

 static EventBusConsumer create(ConsumerOptions options, EventQueue queue,
      EventConsumerRepository consumerRepository) {
   return new EventBusConsumerImpl(options, queue, consumerRepository);
 }

  void start();

  /**
   * 关闭
   */
  void close();

  /**
   * 绑定消息处理类.
   *
   * @param topic
   * @param resource
   * @param consumer
   */
  void consumer(String topic, String resource, EventHandler consumer);

  /**
   * 等待处理的消息数量
   *
   * @return
   */
  long waitForHandle();

  /**
   * 是否运行
   * @return
   */
  boolean isRunning();
}
