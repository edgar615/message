package com.github.edgar615.eventbus.kafka.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.repository.SendEventState;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2018/5/17.
 *
 * @author Edgar  Date 2018/5/17
 */
public class MockEventProducerRepository implements VertxEventProducerRepository {

  private List<Event> events = new CopyOnWriteArrayList<>();


  public List<Event> events() {
    return new ArrayList<>(events);
  }

  @Override
  public void insert(Event event, Handler<AsyncResult<Void>> resultHandler) {
    events.add(event);
    resultHandler.handle(Future.succeededFuture());
  }

  @Override
  public void waitingForSend(int fetchCount,
      Handler<AsyncResult<List<Event>>> resultHandler) {
    List<Event> plist = events.stream().filter(e -> !e.head().ext().containsKey("state"))
        .collect(Collectors.toList());
    plist.forEach(e -> e.head().addExt("state", "0"));
    resultHandler.handle(Future.succeededFuture(new ArrayList<>(plist)));
  }

  @Override
  public void mark(String eventId, SendEventState state, Handler<AsyncResult<Void>> resultHandler) {

  }
}
