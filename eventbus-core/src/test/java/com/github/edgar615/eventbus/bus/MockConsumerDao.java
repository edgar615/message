package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.dao.ConsumeEventState;
import com.github.edgar615.eventbus.dao.EventConsumerDao;
import com.github.edgar615.eventbus.event.Event;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class MockConsumerDao implements EventConsumerDao {

  private final List<Event> events = new CopyOnWriteArrayList<>();

  @Override
  public boolean insert(Event event) {
    event.head().addExt("state", String.valueOf(ConsumeEventState.PENDING.value()));
    events.add(event);
    return false;
  }

  @Override
  public List<Event> waitingForConsume(int fetchCount) {
    return null;
  }

  @Override
  public void mark(String eventId, ConsumeEventState state) {
    events.stream().filter(e -> e.head().id().equals(eventId))
        .forEach(e -> e.head().addExt("state", String.valueOf(state.value())));
  }

  public List<Event> events() {
    return events;
  }
}
