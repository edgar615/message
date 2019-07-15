package com.github.edgar615.eventbus.bus;

import java.util.Map;
import java.util.function.BiPredicate;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
public interface EventBusConsumer {

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
   * @param handler
   */
  void consumer(String topic, String resource, EventSubscriber handler);

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
