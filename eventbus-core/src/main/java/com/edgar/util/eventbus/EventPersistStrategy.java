package com.edgar.util.eventbus;

import com.edgar.util.eventbus.event.Event;

/**
 * Created by edgar on 17-3-23.
 */
public interface EventPersistStrategy {

  void persist(Event event);
}
