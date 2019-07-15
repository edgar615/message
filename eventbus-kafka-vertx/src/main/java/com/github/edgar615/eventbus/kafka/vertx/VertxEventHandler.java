package com.github.edgar615.eventbus.kafka.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.bus.EventSubscriber;
import com.github.edgar615.eventbus.bus.HandlerRegistration;
import io.vertx.core.Future;

import java.util.function.BiPredicate;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
public interface VertxEventHandler extends EventSubscriber {

  void handle(Event event, Future<Void> completeFuture);

  @Override
  default void handle(Event event) {
    Future<Void> completeFuture = Future.future();
    try {
      handle(event, completeFuture);
      completeFuture.complete();
    } catch (Exception e) {
      completeFuture.fail(e);
    }
  }

  default VertxEventHandler register(BiPredicate<String, String> predicate) {
    HandlerRegistration.instance().registerHandler(predicate, this);
    return this;
  }

  default VertxEventHandler register(String topic, String resource) {
    final BiPredicate<String, String> predicate = (t, r) -> {
      boolean topicMatch = true;
      if (topic != null) {
        topicMatch = topic.equals(t);
      }
      boolean resourceMatch = true;
      if (resource != null) {
        resourceMatch = resource.equals(r);
      }
      return topicMatch && resourceMatch;
    };
    register(predicate);
    return this;
  }
}
