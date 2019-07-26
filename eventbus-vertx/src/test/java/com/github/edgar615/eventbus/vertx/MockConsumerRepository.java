package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.github.edgar615.eventbus.repository.ConsumeEventState;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MockConsumerRepository implements VertxEventConsumerRepository {

  private final List<Event> events = new CopyOnWriteArrayList<>();

  private AtomicInteger seq = new AtomicInteger();

  public List<Event> events() {
    return events;
  }

  @Override
  public void insert(Event event, Handler<AsyncResult<Boolean>> resultHandler) {
    event.head().addExt("state", String.valueOf(ConsumeEventState.PENDING.value()));
    events.add(event);
    resultHandler.handle(Future.succeededFuture(false));
  }

  @Override
  public void waitingForConsume(int fetchCount, Handler<AsyncResult<List<Event>>> resultHandler) {
    int min = seq.get();
    int max = min + fetchCount;
    List<Event> events = new ArrayList<>();
    for (int i = min; i < 100; i++) {
      seq.incrementAndGet();
      Message message = Message
          .create("" + i, ImmutableMap.of("foo", "bar", "deviceId", new Random().nextInt(10)));
      Event event = Event.create("DeviceControlEvent", message, 1);
      events.add(event);
      this.events.add(event);
    }
    resultHandler.handle(Future.succeededFuture(events));
  }

  @Override
  public void mark(String eventId, ConsumeEventState state,
      Handler<AsyncResult<Void>> resultHandler) {
    events.stream().filter(e -> e.head().id().equals(eventId))
        .forEach(e -> e.head().addExt("state", String.valueOf(state.value())));
    resultHandler.handle(Future.succeededFuture());
  }
}
