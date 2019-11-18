package com.github.edgar615.message.vertx;

import com.github.edgar615.message.core.Message;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * Created by Edgar on 2019/7/16.
 *
 * @author Edgar  Date 2019/7/16
 */
public interface VertxMessageProducer {

  void send(Message message, Handler<AsyncResult<Message>> resultHandler);

  void save(Message message, Handler<AsyncResult<Void>> resultHandler);

  void close();

  void start();

  int waitForSend();

  static VertxMessageProducer create(Vertx vertx, VertxMessageWriteStream writeStream) {
    return new VertxMessageProducerImpl(vertx, writeStream, null);
  }

  static VertxMessageProducer create(Vertx vertx, VertxMessageWriteStream writeStream,
      VertxMessageProducerRepository producerRepository) {
    return new VertxMessageProducerImpl(vertx, writeStream, producerRepository);
  }
}
