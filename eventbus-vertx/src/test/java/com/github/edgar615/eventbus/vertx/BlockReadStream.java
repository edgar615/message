package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.github.edgar615.eventbus.repository.EventConsumerRepository;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockReadStream extends AbstractVertxEventBusReadStream {

  private AtomicInteger seq = new AtomicInteger();

  public BlockReadStream(EventQueue queue, VertxEventConsumerRepository consumerRepository) {
    super(queue, consumerRepository);
  }

  @Override
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

  @Override
  public void start() {

  }

  @Override
  public void close() {

  }
}
