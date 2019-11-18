package com.github.edgar615.message.vertx;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.SendMessageState;
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
public class MockProducerRepository implements VertxMessageProducerRepository {

  private final List<Message> messages = new ArrayList<>();

  private AtomicInteger pendCount = new AtomicInteger();

  public List<Message> getMessages() {
    return ImmutableList.copyOf(messages);
  }

  public MockProducerRepository addEvent(Message message) {
    messages.add(message);
    return this;
  }

  public int getPendCount() {
    return pendCount.get();
  }

  @Override
  public void insert(Message message, Handler<AsyncResult<Void>> resultHandler) {
    messages.add(message);
    resultHandler.handle(Future.succeededFuture());
  }

  @Override
  public void waitingForSend(Handler<AsyncResult<List<Message>>> resultHandler) {
    pendCount.incrementAndGet();
    List<Message> plist = messages.stream().filter(e -> !e.header().ext().containsKey("state"))
        .collect(Collectors.toList());
    Future<List<Message>> future = Future.future();
    future.complete(plist);
    future.setHandler(resultHandler);
  }

  @Override
  public void waitingForSend(int fetchCount, Handler<AsyncResult<List<Message>>> resultHandler) {
    Future<List<Message>> future = Future.future();
    future.complete(new ArrayList<>());
    future.setHandler(resultHandler);
  }

  @Override
  public void mark(String eventId, SendMessageState state, Handler<AsyncResult<Void>> resultHandler) {
    messages.stream().filter(e -> e.header().id().equalsIgnoreCase(eventId))
        .forEach(e -> e.header().addExt("state", state.value() + ""));
    resultHandler.handle(Future.succeededFuture());
  }
}
