package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

public interface EventHandler {

  void handle(Event event) throws InterruptedException;

}