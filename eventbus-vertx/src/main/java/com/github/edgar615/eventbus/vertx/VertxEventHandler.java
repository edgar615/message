package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface VertxEventHandler {

  void handle(Event event, Handler<AsyncResult<Event>> resultHandler);

}
