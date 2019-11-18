package com.github.edgar615.message.bus;

import com.github.edgar615.message.repository.MessageConsumerRepository;
import com.github.edgar615.message.utils.MessageQueue;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
public interface MessageConsumer {

  static MessageConsumer create(ConsumerOptions options, MessageQueue queue) {
    return new MessageConsumerImpl(options, queue, null);
  }

 static MessageConsumer create(ConsumerOptions options, MessageQueue queue,
      MessageConsumerRepository consumerRepository) {
   return new MessageConsumerImpl(options, queue, consumerRepository);
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
  void consumer(String topic, String resource, MessageHandler consumer);

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
