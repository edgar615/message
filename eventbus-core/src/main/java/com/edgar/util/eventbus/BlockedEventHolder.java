package com.edgar.util.eventbus;

import com.edgar.util.event.Event;

/**
 * Created by Edgar on 2016/7/8.
 *
 * @author Edgar  Date 2016/7/8
 */
class BlockedEventHolder {
  private final Event event;

  private final long maxExecTime;

  private boolean completed;

  private long completedOn;

  private long startedOn = System.currentTimeMillis();

  private BlockedEventHolder(Event event, long maxExecTime) {
    this.event = event;
    this.maxExecTime = maxExecTime;
  }

  static BlockedEventHolder create(Event event, long maxExecTime) {
    return new BlockedEventHolder(event, maxExecTime);
  }

  public long maxExecTime() {
    return maxExecTime;
  }

  public void completed() {
    this.completed = true;
    this.completedOn = System.currentTimeMillis();
  }

  public long duration() {
    return System.currentTimeMillis() - startedOn;
  }

  public Event event() {
    return event;
  }

  public boolean isCompleted() {
    return completed;
  }
}
