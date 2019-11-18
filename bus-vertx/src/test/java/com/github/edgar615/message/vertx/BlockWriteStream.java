package com.github.edgar615.message.vertx;

import com.github.edgar615.message.core.Message;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class BlockWriteStream implements VertxMessageWriteStream {

  private final int block;

  public BlockWriteStream(int block) {
    this.block = block;
  }

  @Override
  public void send(Message message, Handler<AsyncResult<Message>> handler) {
    Future<Message> future = Future.future();
    future.setHandler(handler);
    if (block > 0) {
      try {
        TimeUnit.SECONDS.sleep(block);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    future.complete(message);
  }

  @Override
  public void close() {

  }
}
