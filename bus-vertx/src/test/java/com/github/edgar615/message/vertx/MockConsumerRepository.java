package com.github.edgar615.message.vertx;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.core.Event;
import com.github.edgar615.message.repository.ConsumeMessageState;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MockConsumerRepository implements VertxMessageConsumerRepository {

  private final List<Message> messages = new CopyOnWriteArrayList<>();

  private AtomicInteger seq = new AtomicInteger();

  public List<Message> events() {
    return messages;
  }

  @Override
  public void insert(Message message, Handler<AsyncResult<Boolean>> resultHandler) {
    message.header().addExt("state", String.valueOf(ConsumeMessageState.PENDING.value()));
    messages.add(message);
    resultHandler.handle(Future.succeededFuture(false));
  }

  @Override
  public void waitingForConsume(int fetchCount, Handler<AsyncResult<List<Message>>> resultHandler) {
    int min = seq.get();
    int max = min + fetchCount;
    List<Message> messages = new ArrayList<>();
    for (int i = min; i < 100; i++) {
      seq.incrementAndGet();
      Event event = Event
          .create("" + i, ImmutableMap.of("foo", "bar", "deviceId", new Random().nextInt(10)));
      Message message = Message.create("DeviceControlEvent", event);
      messages.add(message);
      this.messages.add(message);
    }
    resultHandler.handle(Future.succeededFuture(messages));
  }

  @Override
  public void mark(String eventId, ConsumeMessageState state,
      Handler<AsyncResult<Void>> resultHandler) {
    messages.stream().filter(e -> e.header().id().equals(eventId))
        .forEach(e -> e.header().addExt("state", String.valueOf(state.value())));
    resultHandler.handle(Future.succeededFuture());
  }
}
