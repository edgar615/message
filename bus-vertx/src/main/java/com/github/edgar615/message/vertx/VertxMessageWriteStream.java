package com.github.edgar615.message.vertx;

import com.github.edgar615.message.core.Message;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface VertxMessageWriteStream {

  void send(Message message, Handler<AsyncResult<Message>> handler);

  void close();

}
