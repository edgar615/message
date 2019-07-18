package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.bus.EventBusWriteStream;
import com.github.edgar615.eventbus.event.Event;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class BlockWriteStream implements VertxEventBusWriteStream {

  private final int block;

  public BlockWriteStream(int block) {
    this.block = block;
  }

  @Override
  public void send(Event event, Handler<AsyncResult<Event>> handler) {
    Future<Event> future = Future.future();
    future.setHandler(handler);
    if (block > 0) {
      try {
        TimeUnit.SECONDS.sleep(block);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    future.complete(event);
  }

  @Override
  public void close() {

  }
}
