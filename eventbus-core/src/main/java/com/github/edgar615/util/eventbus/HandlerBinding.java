package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.function.BiPredicate;

/**
 * Created by Edgar on 2017/4/12.
 *
 * @author Edgar  Date 2017/4/12
 */
class HandlerBinding {

  private final BiPredicate<String, String> predicate;

  private final EventHandler eventHandler;

  HandlerBinding(BiPredicate<String, String> predicate, EventHandler eventHandler) {
    this.predicate = predicate;
    this.eventHandler = eventHandler;
  }

  public boolean match(Event event) {
    return predicate.test(event.head().to(), event.action().resource());
  }

  public EventHandler eventHandler() {
    return eventHandler;
  }
}
