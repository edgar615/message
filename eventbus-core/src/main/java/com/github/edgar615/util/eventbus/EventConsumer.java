package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
public interface EventConsumer {

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
   * 绑定消息处理类
   *
   * @param predicate
   * @param handler
   */
  void consumer(BiPredicate<String, String> predicate, EventHandler handler);

  /**
   * 绑定消息处理类.
   *
   * @param topic
   * @param resource
   * @param handler
   */
  void consumer(String topic, String resource, EventHandler handler);

  /**
   * 等待处理的消息数量
   *
   * @return
   */
  long waitForHandle();
}
