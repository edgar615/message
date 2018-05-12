package com.github.edgar615.util.eventbus;

import com.google.common.collect.ImmutableList;

import com.github.edgar615.util.event.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by Edgar on 2018/5/12.
 *
 * @author Edgar  Date 2018/5/12
 */
public class MockConsumerStorage implements ConsumerStorage {
  private final List<Event> events = new CopyOnWriteArrayList<>();

  @Override
  public boolean shouldStorage(Event event) {
    return true;
  }

  @Override
  public boolean isConsumed(Event event) {
    return false;
  }

  @Override
  public void mark(Event event, int result) {
    event.head().addExt("status", result + "");
    System.out.println(events.size());
    events.add(event);
  }

  public List<Event> getEvents() {
    return ImmutableList.copyOf(events);
  }

}
