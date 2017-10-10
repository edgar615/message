package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

public interface EventFuture<T> {

  Event event();

  boolean isComplete();

  EventFuture<T> setCallback(Callback<T> callback);

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