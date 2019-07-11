package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;

@Deprecated
public interface EventFuture {

  boolean isComplete();

  EventFuture setCallback(EventCallback callback);

  void complete();

  void fail(Throwable throwable);

  Event event();

  Throwable cause();

  boolean succeeded();

  boolean failed();

  long duration();

  static EventFuture future(Event event) {
    return new EventFutureImpl(event);
  }

}
