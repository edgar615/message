package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Event;
import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.ConsumeMessageState;
import com.github.edgar615.message.repository.MessageConsumerRepository;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MockConsumerRepository implements MessageConsumerRepository {

  private final List<Message> messages = new CopyOnWriteArrayList<>();

  private AtomicInteger seq = new AtomicInteger();

  @Override
  public boolean insert(Message message) {
    message.header().addExt("state", String.valueOf(ConsumeMessageState.PENDING.value()));
    messages.add(message);
    return false;
  }

  @Override
  public List<Message> waitingForConsume(int fetchCount) {
    int min = seq.get();
    int max = min + fetchCount;
    List<Message> messages = new ArrayList<>();
    for (int i = min; i < 100; i++) {
      Event event = Event
          .create("" + i, ImmutableMap.of("foo", "bar", "deviceId", new Random().nextInt(10)));
      Message message = Message.create("DeviceControlEvent", event);
      messages.add(message);
      this.messages.add(message);
    }

    return messages;
  }

  @Override
  public void mark(String eventId, ConsumeMessageState state) {
    messages.stream().filter(e -> e.header().id().equals(eventId))
        .forEach(e -> e.header().addExt("state", String.valueOf(state.value())));
  }

  public List<Message> events() {
    return messages;
  }
}
