package com.github.edgar615.util.eventbus;

import com.google.common.collect.ImmutableList;

import com.github.edgar615.util.event.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class MockProducerStorage implements ProducerStorage {

  private final List<Event> events = new ArrayList<>();

  private AtomicInteger pendCount = new AtomicInteger();

  @Override
  public boolean shouldStorage(Event event) {
    return !event.head().to().equalsIgnoreCase("SMS");
  }

  @Override
  public void save(Event event) {
    events.add(event);
  }

  @Override
  public List<Event> pendingList() {
    pendCount.incrementAndGet();
    List<Event> plist = events.stream().filter(e -> !e.head().ext().containsKey("status"))
            .collect(Collectors.toList());
    plist.forEach(e -> e.head().addExt("status", "0"));
    return new ArrayList<>(plist);
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

  public int getPendCount() {
    return pendCount.get();
  }
}
