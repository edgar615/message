package com.edgar.util.event;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Created by Edgar on 2017/3/8.
 *
 * @author Edgar  Date 2017/3/8
 */
class EventImpl implements Event {

  private final EventHead head;

  private final EventAction action;

  EventImpl(EventHead head, EventAction action) {
    Preconditions.checkNotNull(head, "head can not be null");
    Preconditions.checkNotNull(action, "action can not be null");
    this.head = head;
    this.action = action;
  }

  @Override
  public EventHead head() {
    return head;
  }

  @Override
  public EventAction action() {
    return action;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Event")
        .add("head", head)
        .add("data", action)
        .toString();
  }
}
