package com.edgar.util.eventbus;

import com.edgar.util.event.Event;

public interface EventHandler {

  void handle(Event event);

}