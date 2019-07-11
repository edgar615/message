package com.github.edgar615.eventbus.bus;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class SubscriberRegistry {

  private Multimap<SubscriberRegistryKey, Subscriber> registry = ArrayListMultimap.create();

  void register(Object listener) {

  }
}
