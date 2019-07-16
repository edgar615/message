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
class ConsumerRegistry {

  private static final ConsumerRegistry INSTANCE = new ConsumerRegistry();

  private final ConcurrentMap<ConsumerKey, CopyOnWriteArraySet<EventConsumer>> subscribers =
      Maps.newConcurrentMap();

  private ConsumerRegistry() {
  }

  static ConsumerRegistry instance() {
    return INSTANCE;
  }

  void register(ConsumerKey key, EventConsumer subscriber) {
    Collection<EventConsumer> eventConsumers = subscribers.get(key);
    if (eventConsumers == null) {
      CopyOnWriteArraySet<EventConsumer> newSet = new CopyOnWriteArraySet<>();
      eventConsumers =
          MoreObjects.firstNonNull(subscribers.putIfAbsent(key, newSet), newSet);
    }
    eventConsumers.add(subscriber);
  }

  void unregister(ConsumerKey key, EventConsumer subscriber) {
    Collection<EventConsumer> eventConsumers = subscribers.get(key);
    if (eventConsumers == null) {
      return;
    }
    eventConsumers.remove(subscriber);
  }

  void unregisterAll(ConsumerKey key) {
    subscribers.remove(key);
  }

  Collection<EventConsumer> findAllSubscribers(ConsumerKey key) {
    return subscribers.keySet().stream()
        .filter(registerKey -> this.match(registerKey, key))
        .flatMap(registerKey -> subscribers.getOrDefault(registerKey, new CopyOnWriteArraySet<>()).stream())
        .collect(Collectors.toList());
  }

  private boolean match(ConsumerKey registerKey, ConsumerKey eventKey) {
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
