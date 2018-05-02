package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;
import com.google.common.base.Preconditions;

import java.util.LinkedList;

public class DefaultEventQueue implements EventQueue {
  /**
   * 任务列表
   */
  private final LinkedList<Event> events = new LinkedList<>();

  private final int limit;

  public DefaultEventQueue() {
    this.limit = Integer.MAX_VALUE;
  }

  public DefaultEventQueue(int limit) {
    Preconditions.checkArgument(limit > 0);
    this.limit = limit;
  }

  @Override
  public synchronized Event dequeue() {
    return events.poll();
  }

  @Override
  public synchronized boolean enqueue(Event event) {
    events.add(event);
    if (this.events.size() >= limit) {
      return false;
    }
    return true;
  }

  @Override
  public synchronized void complete(Event event) {
    //do nothing
  }

  @Override
  public synchronized int size() {
    return limit;
  }
}
