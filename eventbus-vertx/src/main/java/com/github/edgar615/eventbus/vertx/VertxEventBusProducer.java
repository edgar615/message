package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Created by Edgar on 2019/7/16.
 *
 * @author Edgar  Date 2019/7/16
 */
public interface VertxEventBusProducer {

  void send(Event event, Handler<AsyncResult<Event>> resultHandler);

  void save(Event event, Handler<AsyncResult<Void>> resultHandler);

  void close();

  int waitForSend();

//  static VertxKafkaEventbusProducer create(Vertx vertx, KafkaWriteOptions options) {
//    return new VertxKafkaEventbusProducerImpl(vertx, options);
//  }
//
//  static VertxKafkaEventbusProducer create(Vertx vertx, KafkaWriteOptions options,
//      VertxEventProducerRepository storage) {
//    return new VertxKafkaEventbusProducerImpl(vertx, options, storage);
//  }
}
