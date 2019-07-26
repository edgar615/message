package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.github.edgar615.eventbus.repository.ConsumeEventState;
import com.github.edgar615.eventbus.repository.EventConsumerRepository;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MockConsumerRepository implements EventConsumerRepository {

  private final List<Event> events = new CopyOnWriteArrayList<>();

  private AtomicInteger seq = new AtomicInteger();

  @Override
  public boolean insert(Event event) {
    event.head().addExt("state", String.valueOf(ConsumeEventState.PENDING.value()));
    events.add(event);
    return false;
  }

  @Override
  public List<Event> waitingForConsume(int fetchCount) {
    int min = seq.get();
    int max = min + fetchCount;
    List<Event> events = new ArrayList<>();
    for (int i = min; i < 100; i++) {
      Message message = Message
          .create("" + i, ImmutableMap.of("foo", "bar", "deviceId", new Random().nextInt(10)));
      Event event = Event.create("DeviceControlEvent", message, 1);
      events.add(event);
      this.events.add(event);
    }

    return events;
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
