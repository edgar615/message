package com.edgar.util.eventbus.kafka;

import com.google.common.collect.ImmutableList;

import com.edgar.util.event.Event;
import com.edgar.util.eventbus.SendStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class MockSendStorage implements SendStorage {

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

  public MockSendStorage addEvent(Event event) {
    events.add(event);
    return this;
  }

}
