package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

public interface Partitioner {

  int partition(Event event);
}