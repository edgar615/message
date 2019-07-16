package com.github.edgar615.eventbus.bus;

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
class HandlerRegistry {

  private static final HandlerRegistry INSTANCE = new HandlerRegistry();

  private final ConcurrentMap<HandlerKey, CopyOnWriteArraySet<EventHandler>> handlers =
      Maps.newConcurrentMap();

  private HandlerRegistry() {
  }

  static HandlerRegistry instance() {
    return INSTANCE;
  }

  void register(HandlerKey key, EventHandler handler) {
    Collection<EventHandler> eventHandlers = handlers.get(key);
    if (eventHandlers == null) {
      CopyOnWriteArraySet<EventHandler> newSet = new CopyOnWriteArraySet<>();
      eventHandlers =
          MoreObjects.firstNonNull(handlers.putIfAbsent(key, newSet), newSet);
    }
    eventHandlers.add(handler);
  }

  void unregister(HandlerKey key, EventHandler handler) {
    Collection<EventHandler> eventHandlers = handlers.get(key);
    if (eventHandlers == null) {
      return;
    }
    eventHandlers.remove(handler);
  }

  void unregisterAll(HandlerKey key) {
    handlers.remove(key);
  }

  Collection<EventHandler> findAllHandler(HandlerKey key) {
    return handlers.keySet().stream()
        .filter(registerKey -> this.match(registerKey, key))
        .flatMap(registerKey -> handlers.getOrDefault(registerKey, new CopyOnWriteArraySet<>()).stream())
        .collect(Collectors.toList());
  }

  private boolean match(HandlerKey registerKey, HandlerKey eventKey) {
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
