package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;

/**
 * Created by Edgar on 2018/5/12.
 *
 * @author Edgar  Date 2018/5/12
 */
public interface ConsumerStorage {

  /**
   * 过滤不需要持久化的事件
   *
   * @param event 事件
   * @return true：持久化，false：不做持久化
   */
  boolean shouldStorage(Event event);

  /**
   * 判断事件是否已经消费
   *
   * @param event
   * @return
   */
  boolean isConsumed(Event event);

  /**
   * 标记事件
   *
   * @param event
   * @param result 1-消费成功 2-消费失败 3-黑名单
   */
  void mark(Event event, int result);
}
