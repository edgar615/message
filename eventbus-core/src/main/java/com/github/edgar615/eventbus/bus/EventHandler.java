package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;

public interface EventHandler {

  void handle(Event event);

}
