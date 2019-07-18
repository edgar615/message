package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.repository.EventProducerRepository;
import com.github.edgar615.eventbus.repository.SendEventState;
import com.google.common.collect.ImmutableList;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class MockProducerRepository implements VertxEventProducerRepository {

  private final List<Event> events = new ArrayList<>();

  private AtomicInteger pendCount = new AtomicInteger();

  public List<Event> getEvents() {
    return ImmutableList.copyOf(events);
  }

  public MockProducerRepository addEvent(Event event) {
    events.add(event);
    return this;
  }

  public int getPendCount() {
    return pendCount.get();
  }

  @Override
  public void insert(Event event, Handler<AsyncResult<Void>> resultHandler) {
    events.add(event);
    resultHandler.handle(Future.succeededFuture());
  }

  @Override
  public void waitingForSend(Handler<AsyncResult<List<Event>>> resultHandler) {
    pendCount.incrementAndGet();
    List<Event> plist = events.stream().filter(e -> !e.head().ext().containsKey("state"))
        .collect(Collectors.toList());
    Future<List<Event>> future = Future.future();
    future.complete(plist);
    future.setHandler(resultHandler);
  }

  @Override
  public void waitingForSend(int fetchCount, Handler<AsyncResult<List<Event>>> resultHandler) {
    Future<List<Event>> future = Future.future();
    future.complete(new ArrayList<>());
    future.setHandler(resultHandler);
  }

  @Override
  public void mark(String eventId, SendEventState state, Handler<AsyncResult<Void>> resultHandler) {
    events.stream().filter(e -> e.head().id().equalsIgnoreCase(eventId))
        .forEach(e -> e.head().addExt("state", state.value() + ""));
    resultHandler.handle(Future.succeededFuture());
  }
}
