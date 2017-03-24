package com.edgar.util.eventbus;

import com.edgar.util.eventbus.event.Event;

public interface EventFuture<T> {

  Event event();

  boolean isComplete();

  EventFuture<T> setCallback(Callback callback);

  void complete(T result);

  void fail(Throwable throwable);

  T result();

  Throwable cause();

  boolean succeeded();

  boolean failed();

  static <T> EventFuture<T> future(Event event) {
    return new EventFutureImpl<>(event);
  }
}