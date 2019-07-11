package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;

/**
 * Created by Edgar on 2016/4/26.
 *
 * @author Edgar  Date 2016/4/26
 */
class EventFutureImpl implements EventFuture {

  private final Event event;

  private boolean failed;

  private boolean succeeded;

  private Throwable throwable;

  private EventCallback callback;

  private long start = System.currentTimeMillis();

  private long duration;

  EventFutureImpl(Event event) {
    this.event = event;
  }

  @Override
  public Event event() {
    return event;
  }

  @Override
  public Throwable cause() {
    return throwable;
  }

  @Override
  public boolean succeeded() {
    return succeeded;
  }

  @Override
  public boolean failed() {
    return failed;
  }

  @Override
  public boolean isComplete() {
    return failed || succeeded;
  }

  @Override
  public EventFuture setCallback(EventCallback callback) {
    this.callback = callback;
    checkCallback();
    return this;
  }

  @Override
  public void complete() {
    checkComplete();
    this.succeeded = true;
    this.duration = System.currentTimeMillis() - start;
    checkCallback();
  }

  @Override
  public void fail(Throwable throwable) {
    checkComplete();
    this.throwable = throwable;
    this.failed = true;
    this.duration = System.currentTimeMillis() - start;
    checkCallback();
  }

  @Override
  public long duration() {
    return duration;
  }

  private void checkCallback() {
    if (callback != null && isComplete()) {
      callback.onCallBack(this);
    }
  }

  private void checkComplete() {
    if (succeeded || failed) {
      throw new IllegalStateException(
              "Result is already complete: " + (succeeded ? "succeeded" : "failed:" + throwable));
    }
  }
}
