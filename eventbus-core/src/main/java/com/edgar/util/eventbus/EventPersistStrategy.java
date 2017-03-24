package com.edgar.util.eventbus;

import com.edgar.util.eventbus.event.Event;

/**
 * 需要发送的消息可以通过这个接口实现持久化.
 */
public interface EventPersistStrategy {

  boolean shouldPersist(Event event);

  void persist(Event event);
}
