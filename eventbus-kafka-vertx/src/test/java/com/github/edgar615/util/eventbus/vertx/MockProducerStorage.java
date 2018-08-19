package com.github.edgar615.util.eventbus.vertx;

import com.github.edgar615.util.event.Event;
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
public class MockProducerStorage implements VertxProducerStorage {

  private List<Event> events = new CopyOnWriteArrayList<>();


  @Override
  public boolean shouldStorage(Event event) {
    return true;
  }

  @Override
  public void save(Event event, Handler<AsyncResult<Void>> resultHandler) {
    events.add(event);
    resultHandler.handle(Future.succeededFuture());
  }

  @Override
  public void pendingList(Handler<AsyncResult<List<Event>>> resultHandler) {
    List<Event> plist = events.stream().filter(e -> !e.head().ext().containsKey("status"))
            .collect(Collectors.toList());
    plist.forEach(e -> e.head().addExt("status", "0"));
    resultHandler.handle(Future.succeededFuture(new ArrayList<>(plist)));
  }

  @Override
  public void mark(Event event, int status, Handler<AsyncResult<Void>> resultHandler) {

  }

  public List<Event> events() {
    return new ArrayList<>(events);
  }
}
