package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.repository.EventConsumerRepository;
import com.github.edgar615.eventbus.utils.EventQueue;
import java.util.Map;

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
      EventConsumerRepository consumerDao) {
   return new EventBusConsumerImpl(options, queue, consumerDao);
 }

  void start();

  /**
   * 关闭
   */
  void close();

  /**
   * 度量指标
   *
   * @return
   */
  Map<String, Object> metrics();

  /**
   * 绑定消息处理类.
   *
   * @param topic
   * @param resource
   * @param consumer
   */
  void consumer(String topic, String resource, EventConsumer consumer);

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
