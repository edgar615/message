package com.edgar.util.eventbus;

import com.google.common.collect.ImmutableList;

import com.edgar.util.event.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class MockProducerStorage implements ProducerStorage {

  private final List<Event> events = new ArrayList<>();

  @Override
  public boolean shouldStorage(Event event) {
    return !event.head().to().equalsIgnoreCase("SMS");
  }

  @Override
  public void save(Event event) {
    event.head().addExt("status", "0");
    events.add(event);
  }

  @Override
  public List<Event> pendingList() {
    return events.stream().filter(e -> e.head().ext("status").equalsIgnoreCase("0"))
            .collect(Collectors.toList());
  }

  @Override
  public void mark(Event event, int status) {
    events.stream().filter(e -> e.head().id().equalsIgnoreCase(event.head().id()))
            .forEach(e -> e.head().addExt("status", status + ""));
  }

  public List<Event> getEvents() {
    return ImmutableList.copyOf(events);
  }

  public MockProducerStorage addEvent(Event event) {
    events.add(event);
    return this;
  }

}
