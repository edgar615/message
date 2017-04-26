package com.edgar.util.eventbus;

import com.edgar.util.event.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/4/14.
 *
 * @author Edgar  Date 2017/4/14
 */
public class HandlerRegistration {
  private static final HandlerRegistration INSTANCE = new HandlerRegistration();

  private final List<HandlerBinding> bindings = new ArrayList<>();

  private HandlerRegistration() {
  }

  public static HandlerRegistration instance() {
    return INSTANCE;
  }

  public void registerHandler(BiPredicate<String, String> predicate, EventHandler handler) {
    HandlerBinding binding = new HandlerBinding(predicate, handler);
    bindings.add(binding);
  }

  public List<EventHandler> getHandlers(Event event) {
    return bindings.stream()
            .filter(b -> b.match(event))
            .map(b -> b.eventHandler())
            .collect(Collectors.toList());
  }
}
