package com.github.edgar615.eventbus.bus;

public class SubscriberRegistryKey {

  private final String topic;

  private final String resource;

  public static SubscriberRegistryKey create(String topic, String resource) {
    return new SubscriberRegistryKey(topic, resource);
  }

  private SubscriberRegistryKey(String topic, String resource) {
    this.topic = topic;
    this.resource = resource;
  }

  public String topic() {
    return topic;
  }

  public String resource() {
    return resource;
  }
}
