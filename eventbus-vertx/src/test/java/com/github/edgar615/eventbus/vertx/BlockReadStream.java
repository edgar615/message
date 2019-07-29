package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockReadStream extends AbstractVertxEventBusReadStream {

  private AtomicInteger seq = new AtomicInteger();

  public BlockReadStream(Vertx vertx, EventQueue queue, VertxEventConsumerRepository consumerRepository) {
    super(vertx, queue, consumerRepository);
  }


  @Override
  public void start() {

  }

  @Override
  public void close() {

  }

  public void poll(Handler<AsyncResult<List<Event>>> handler) {
    int min = seq.get();
    int max = min + 100;
    List<Event> events = new ArrayList<>();
    for (int i = min; i < max; i++) {
      Message message = Message
          .create("" + i, ImmutableMap.of("foo", "bar", "deviceId", new Random().nextInt(10)));
      Event event = Event.create("DeviceControlEvent", message, 1);
      events.add(event);
    }
    handler.handle(Future.succeededFuture(events));
  }


  public final void pollAndEnqueue(Handler<AsyncResult<Integer>> handler) {
    poll(ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      enqueue(ar.result(), handler);
    });

  }

}
