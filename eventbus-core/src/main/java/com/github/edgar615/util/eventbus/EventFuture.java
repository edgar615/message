package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

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