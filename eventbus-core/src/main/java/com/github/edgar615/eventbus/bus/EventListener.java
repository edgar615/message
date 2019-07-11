package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;

public interface EventListener {

  void subscribe(Event event);
}
