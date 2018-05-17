package com.github.edgar615.util.eventbus.vertx;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.eventbus.KafkaProducerOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.Map;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
public interface KafkaVertxEventbusProducer {

  void send(Event event, Handler<AsyncResult<Void>> resultHandler);

  void close();

  int waitForSend();

  Map<String, Object> metrics();

  static KafkaVertxEventbusProducer create(Vertx vertx, KafkaProducerOptions options) {
    return new KafkaVertxEventbusProducerImpl(vertx, options);
  }

  static KafkaVertxEventbusProducer create(Vertx vertx, KafkaProducerOptions options,
                                           VertxProducerStorage storage) {
    return new KafkaVertxEventbusProducerImpl(vertx, options, storage);
  }
}
