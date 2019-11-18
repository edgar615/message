package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Message;
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
  public CompletableFuture<Message> send(Message message) {
    CompletableFuture<Message> future = new CompletableFuture<>();

    if (seq.getAndIncrement() % 2 == 0) {
      future.complete(message);
    } else {
      future.completeExceptionally(new RuntimeException("failed"));
    }
    return future;
  }

  @Override
  public void start() {

  }

  @Override
  public void close() {

  }
}
