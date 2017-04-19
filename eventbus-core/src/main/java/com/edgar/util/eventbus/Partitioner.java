package com.edgar.util.eventbus;

import com.edgar.util.event.Event;

public interface Partitioner {

  int partition(Event event);
}