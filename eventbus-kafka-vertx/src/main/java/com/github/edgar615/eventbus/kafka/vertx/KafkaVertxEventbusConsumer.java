package com.github.edgar615.eventbus.kafka.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.kafka.KafkaConsumerOptions;
import io.vertx.core.Vertx;

import java.util.Map;
import java.util.function.Function;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
public interface KafkaVertxEventbusConsumer {
  void pause();

  void resume();

  void close();

  long waitForHandle();

  Map<String, Object> metrics();

  boolean isRunning();

  boolean paused();


  static KafkaVertxEventbusConsumer create(Vertx vertx, KafkaConsumerOptions options) {
    return new KafkaVertxEventbusConsumerImpl(vertx, options);
  }

  static KafkaVertxEventbusConsumer create(Vertx vertx, KafkaConsumerOptions options,
                                           VertxConsumerStorage consumerStorage,
                                           Function<Event, String> identificationExtractor,
                                           Function<Event, Boolean> blackListFilter) {
    return new KafkaVertxEventbusConsumerImpl(vertx, options, consumerStorage,
                                              identificationExtractor, blackListFilter);
  }
}
