package com.edgar.util.eventbus;

import com.edgar.util.event.Event;

/**
 * Created by Edgar on 2016/4/26.
 *
 * @author Edgar  Date 2016/4/26
 */
class EventFutureImpl<T> implements EventFuture<T> {

  private final Event event;

  private boolean failed;

  private boolean succeeded;

  private T result;

  private Throwable throwable;

  private Callback callback;

  EventFutureImpl(Event event) {
    this.event = event;
  }


  @Override
  public T result() {
    return result;
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
  public Event event() {
    return event;
  }

  @Override
  public boolean isComplete() {
    return failed || succeeded;
  }

  @Override
  public EventFuture<T> setCallback(Callback callback) {
    this.callback = callback;
    checkCallback();
    return this;
  }

  @Override
  public void complete(T result) {
    checkComplete();
    this.result = result;
    succeeded = true;
    checkCallback();
  }

  @Override
  public void fail(Throwable throwable) {
    checkComplete();
    this.throwable = throwable;
    failed = true;
    checkCallback();
  }

  private void checkCallback() {
    if (callback != null && isComplete()) {
      callback.onCallBack(this);
    }
  }

  private void checkComplete() {
    if (succeeded || failed) {
      throw new IllegalStateException(
              "id:" + event.head().id()
              + "Result is already complete: " + (succeeded ? "succeeded" : "failed:" + throwable));
    }
  }
}
