package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

public interface EventQueue {

  Event dequeue();

  boolean enqueue(Event event);

  void complete(Event event);

  int size();
}
