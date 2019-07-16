package com.github.edgar615.eventbus.vertx;

/**
 * Created by Edgar on 2019/7/16.
 *
 * @author Edgar  Date 2019/7/16
 */
public interface VertxEventBusConsumer {

//  static VertxEventBusConsumer create(ConsumerOptions options, EventQueue queue) {
//    return new EventBusConsumerImpl(options, queue, null);
//  }
//
// static VertxEventBusConsumer create(ConsumerOptions options, EventQueue queue,
//     EventConsumerRepository consumerRepository) {
//   return new EventBusConsumerImpl(options, queue, consumerRepository);
// }

  void start();

  /**
   * 关闭
   */
  void close();

  /**
   * 绑定消息处理类.
   */
  void consumer(String topic, String resource, VertxEventHandler handler);

  /**
   * 等待处理的消息数量
   */
  long waitForHandle();

  /**
   * 是否运行
   */
  boolean isRunning();
}
