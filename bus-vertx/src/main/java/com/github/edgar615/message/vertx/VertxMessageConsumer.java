package com.github.edgar615.message.vertx;

import com.github.edgar615.message.bus.ConsumerOptions;
import com.github.edgar615.message.utils.MessageQueue;
import io.vertx.core.Vertx;

/**
 * Created by Edgar on 2019/7/16.
 *
 * @author Edgar  Date 2019/7/16
 */
public interface VertxMessageConsumer {

  static VertxMessageConsumer create(Vertx vertx, ConsumerOptions options, MessageQueue queue) {
    return new VertxMessageConsumreImpl(vertx, options, queue, null);
  }

  static VertxMessageConsumer create(Vertx vertx, ConsumerOptions options, MessageQueue queue,
      VertxMessageConsumerRepository consumerRepository) {
    return new VertxMessageConsumreImpl(vertx, options, queue, consumerRepository);
  }

  void start();

  /**
   * 关闭
   */
  void close();

  /**
   * 绑定消息处理类.
   */
  void consumer(String topic, String resource, VertxMessageHandler handler);

  /**
   * 等待处理的消息数量
   */
  long waitForHandle();

  /**
   * 是否运行
   */
  boolean isRunning();
}
