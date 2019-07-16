package com.github.edgar615.eventbus.vertx;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

/**
 * 借鉴guava的eventbus代码.
 *
 * @author Edgar  Date 2017/4/14
 */
class VertxHandlerRegistry {

  private static final VertxHandlerRegistry INSTANCE = new VertxHandlerRegistry();

  private final ConcurrentMap<VertxHandlerKey, CopyOnWriteArraySet<VertxEventHandler>> handlers =
      Maps.newConcurrentMap();

  private VertxHandlerRegistry() {
  }

  static VertxHandlerRegistry instance() {
    return INSTANCE;
  }

  void register(VertxHandlerKey key, VertxEventHandler handler) {
    Collection<VertxEventHandler> eventHandlers = handlers.get(key);
    if (eventHandlers == null) {
      CopyOnWriteArraySet<VertxEventHandler> newSet = new CopyOnWriteArraySet<>();
      eventHandlers =
          MoreObjects.firstNonNull(handlers.putIfAbsent(key, newSet), newSet);
    }
    eventHandlers.add(handler);
  }

  void unregister(VertxHandlerKey key, VertxEventHandler handler) {
    Collection<VertxEventHandler> eventHandlers = handlers.get(key);
    if (eventHandlers == null) {
      return;
    }
    eventHandlers.remove(handler);
  }

  void unregisterAll(VertxHandlerKey key) {
    handlers.remove(key);
  }

  Collection<VertxEventHandler> findAllHandler(VertxHandlerKey key) {
    return handlers.keySet().stream()
        .filter(registerKey -> this.match(registerKey, key))
        .flatMap(
            registerKey -> handlers.getOrDefault(registerKey, new CopyOnWriteArraySet<>()).stream())
        .collect(Collectors.toList());
  }

  private boolean match(VertxHandlerKey registerKey, VertxHandlerKey eventKey) {
    boolean topicMatch = true;
    if (registerKey.topic() != null) {
      topicMatch = registerKey.topic().equals(eventKey.topic());
    }
    boolean resourceMatch = true;
    if (registerKey.resource() != null) {
      resourceMatch = registerKey.resource().equals(eventKey.resource());
    }
    return topicMatch && resourceMatch;
  }
}
