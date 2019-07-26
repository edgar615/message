package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class RoundRobinWriteStream implements VertxEventBusWriteStream {

  private final AtomicInteger seq = new AtomicInteger(0);

  @Override
  public void send(Event event, Handler<AsyncResult<Event>> handler) {
    Future<Event> future = Future.future();
    future.setHandler(handler);

    if (seq.getAndIncrement() % 2 == 0) {
      future.complete(event);
    } else {
      future.fail(new RuntimeException("failed"));
    }
  }

  @Override
  public void close() {

  }
}
