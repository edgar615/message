package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import io.vertx.core.AsyncResult;

public interface VertxEventHandler {

  void subscribe(Event event, AsyncResult<Event> resultHandler);

}
