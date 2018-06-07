package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

/**
 * Created by Edgar on 2016/7/8.
 *
 * @author Edgar  Date 2016/7/8
 */
class BlockedEventHolder {
  private final String eventId;

  private final long maxExecTime;

  private boolean completed;

  private long completedOn;

  private long startedOn = System.currentTimeMillis();

  private BlockedEventHolder(String eventId, long maxExecTime) {
    this.eventId = eventId;
    this.maxExecTime = maxExecTime;
  }

  static BlockedEventHolder create(String eventId, long maxExecTime) {
    return new BlockedEventHolder(eventId, maxExecTime);
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

  public String eventId() {
    return eventId;
  }

  public boolean isCompleted() {
    return completed;
  }
}
