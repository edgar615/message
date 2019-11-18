package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Message;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class BlockWriteStream implements MessageWriteStream {

  private final int block;

  public BlockWriteStream(int block) {
    this.block = block;
  }

  @Override
  public CompletableFuture<Message> send(Message message) {
    CompletableFuture<Message> future = new CompletableFuture<>();
    if (block > 0) {
      try {
        TimeUnit.SECONDS.sleep(block);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    future.complete(message);
    return future;
  }

  @Override
  public void start() {

  }

  @Override
  public void close() {

  }
}
