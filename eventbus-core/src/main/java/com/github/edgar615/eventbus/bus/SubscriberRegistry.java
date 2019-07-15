package com.github.edgar615.eventbus.bus;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 借鉴guava的eventbus代码.
 *
 * @author Edgar  Date 2017/4/14
 */
class SubscriberRegistry {

  private static final SubscriberRegistry INSTANCE = new SubscriberRegistry();

  private final ConcurrentMap<SubscriberKey, CopyOnWriteArraySet<EventSubscriber>> subscribers =
      Maps.newConcurrentMap();

  private SubscriberRegistry() {
  }

  static SubscriberRegistry instance() {
    return INSTANCE;
  }

  void register(SubscriberKey key, EventSubscriber subscriber) {
    Collection<EventSubscriber> eventSubscribers = findAllSubscribers(key);
    if (eventSubscribers == null) {
      CopyOnWriteArraySet<EventSubscriber> newSet = new CopyOnWriteArraySet<>();
      eventSubscribers =
          MoreObjects.firstNonNull(subscribers.putIfAbsent(key, newSet), newSet);
    }
    eventSubscribers.add(subscriber);
  }

  void unregister(SubscriberKey key, EventSubscriber subscriber) {
    Collection<EventSubscriber> eventSubscribers = findAllSubscribers(key);
    if (eventSubscribers == null) {
      return;
    }
    eventSubscribers.remove(subscriber);
  }

  Collection<EventSubscriber> findAllSubscribers(SubscriberKey key) {
    return subscribers.get(key);
  }
}
