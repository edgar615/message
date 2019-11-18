package com.github.edgar615.message.vertx;

import com.github.edgar615.message.core.Event;
import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.utils.MessageQueue;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockReadStream extends AbstractVertxMessageReadStream {

  private AtomicInteger seq = new AtomicInteger();

  public BlockReadStream(Vertx vertx, MessageQueue queue, VertxMessageConsumerRepository consumerRepository) {
    super(vertx, queue, consumerRepository);
  }


  @Override
  public void start() {

  }

  @Override
  public void close() {

  }

  public void poll(Handler<AsyncResult<List<Message>>> handler) {
    int min = seq.get();
    int max = min + 100;
    List<Message> messages = new ArrayList<>();
    for (int i = min; i < max; i++) {
      Event event = Event
          .create("" + i, ImmutableMap.of("foo", "bar", "deviceId", new Random().nextInt(10)));
      Message message = Message.create("DeviceControlEvent", event, 1);
      messages.add(message);
    }
    handler.handle(Future.succeededFuture(messages));
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
