package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class BlockWriteStream implements EventBusWriteStream {

  private final int block;

  public BlockWriteStream(int block) {
    this.block = block;
  }

  @Override
  public CompletableFuture<Event> send(Event event) {
    CompletableFuture<Event> future = new CompletableFuture<>();
    if (block > 0) {
      try {
        TimeUnit.SECONDS.sleep(block);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    future.complete(event);
    return future;
  }

  @Override
  public void close() {

  }
}
