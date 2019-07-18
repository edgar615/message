package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * Created by Edgar on 2019/7/16.
 *
 * @author Edgar  Date 2019/7/16
 */
public interface VertxEventBusProducer {

  void send(Event event, Handler<AsyncResult<Event>> resultHandler);

  void save(Event event, Handler<AsyncResult<Void>> resultHandler);

  void close();

  void start();

  int waitForSend();

  static VertxEventBusProducer create(Vertx vertx, VertxEventBusWriteStream writeStream) {
    return new VertxEventBusProducerImpl(vertx, writeStream, null);
  }

  static VertxEventBusProducer create(Vertx vertx, VertxEventBusWriteStream writeStream,
      VertxEventProducerRepository producerRepository) {
    return new VertxEventBusProducerImpl(vertx, writeStream, producerRepository);
  }
}
