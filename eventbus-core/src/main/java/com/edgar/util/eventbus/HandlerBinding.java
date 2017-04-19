package com.edgar.util.eventbus;

import com.edgar.util.event.Event;

/**
 * Created by Edgar on 2017/4/12.
 *
 * @author Edgar  Date 2017/4/12
 */
class HandlerBinding {

  private final String topic;

  private final String resource;

  private final EventHandler eventHandler;

  HandlerBinding(String topic, String resource, EventHandler eventHandler) {
    this.topic = topic;
    this.resource = resource;
    this.eventHandler = eventHandler;
  }

  public boolean match(Event event) {
    boolean topicMatch = true;
    if (topic != null) {
      topicMatch = topic.equals(event.head().to());
    }
    boolean resourceMatch = true;
    if (resource != null) {
      resourceMatch = resource.equals(event.action().resource());
    }
    return topicMatch && resourceMatch;
  }

  public String topic() {
    return topic;
  }

  public String resource() {
    return resource;
  }

  public EventHandler eventHandler() {
    return eventHandler;
  }
}
