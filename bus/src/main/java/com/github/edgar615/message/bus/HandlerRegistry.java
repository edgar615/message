package com.github.edgar615.message.bus;

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

  private final ConcurrentMap<HandlerKey, CopyOnWriteArraySet<MessageHandler>> handlers =
      Maps.newConcurrentMap();

  private HandlerRegistry() {
  }

  static HandlerRegistry instance() {
    return INSTANCE;
  }

  void register(HandlerKey key, MessageHandler handler) {
    Collection<MessageHandler> messageHandlers = handlers.get(key);
    if (messageHandlers == null) {
      CopyOnWriteArraySet<MessageHandler> newSet = new CopyOnWriteArraySet<>();
      messageHandlers =
          MoreObjects.firstNonNull(handlers.putIfAbsent(key, newSet), newSet);
    }
    messageHandlers.add(handler);
  }

  void unregister(HandlerKey key, MessageHandler handler) {
    Collection<MessageHandler> messageHandlers = handlers.get(key);
    if (messageHandlers == null) {
      return;
    }
    messageHandlers.remove(handler);
  }

  void unregisterAll(HandlerKey key) {
    handlers.remove(key);
  }

  Collection<MessageHandler> findAllHandler(HandlerKey key) {
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
