package com.edgar.util.eventbus.event;

import com.google.common.base.Preconditions;

/**
 * Created by Edgar on 2017/3/8.
 *
 * @author Edgar  Date 2017/3/8
 */
public interface Event {

  /**
   * @return 消息头
   */
  EventHead head();

  /**
   * @return 消息活动
   */
  EventAction action();

  /**
   * 创建一个Event对象
   *
   * @param head   消息头
   * @param action 消息活动
   * @return Event对象
   */
  static Event create(EventHead head, EventAction action) {
    return new EventImpl(head, action);
  }

  /**
   * 创建一个Event对象
   *
   * @param to     消息接收者信道
   * @param action 消息活动
   * @return Event对象
   */
  static Event create(String to, EventAction action) {
    Preconditions.checkNotNull(action, "action cannot be null");
    EventHead head = EventHead.create(to, action.name());
    return create(head, action);
  }

  /**
   * 创建一个Event对象
   *
   * @param to     消息接收者信道
   * @param action 消息活动
   * @return Event对象
   */
  static Event create(String to, EventAction action, long duration) {
    Preconditions.checkNotNull(action, "action cannot be null");
    EventHead head = EventHead.create(to, action.name(), duration);
    return create(head, action);
  }

  /**
   * 创建一个Event对象
   *
   * @param id     消息ID，消息ID请使用唯一的ID
   * @param to     消息接收者信道
   * @param action 消息活动
   * @return Event对象
   */
  static Event create(String id, String to, EventAction action) {
    Preconditions.checkNotNull(action, "action cannot be null");
    EventHead head = EventHead.create(id, to, action.name());
    return create(head, action);
  }

  /**
   * 创建一个Event对象
   *
   * @param id       消息ID，消息ID请使用唯一的ID
   * @param to       消息接收者信道
   * @param action   消息活动
   * @param duration 多长时间有效，单位秒，小于0为永不过期
   * @return
   */
  static Event create(String id, String to, EventAction action,
                                           long duration) {
    Preconditions.checkNotNull(action, "action cannot be null");
    EventHead head = EventHead.create(id, to, action.name(), duration);
    return create(head, action);
  }
}
