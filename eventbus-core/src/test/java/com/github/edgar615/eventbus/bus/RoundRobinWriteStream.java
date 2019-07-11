package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.bus.EventBusWriteStream;
import com.github.edgar615.eventbus.event.Event;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class RoundRobinWriteStream implements EventBusWriteStream {

  private final AtomicInteger seq = new AtomicInteger(0);

  @Override
  public CompletableFuture<Event> send(Event event) {
    CompletableFuture<Event> future = new CompletableFuture<>();

    if (seq.getAndIncrement() % 2 == 0) {
      future.complete(event);
    } else {
      future.completeExceptionally(new RuntimeException("failed"));
    }
    return future;
  }

  @Override
  public void close() {

  }
}
