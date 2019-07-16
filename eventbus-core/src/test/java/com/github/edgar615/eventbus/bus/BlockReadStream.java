package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.repository.EventConsumerRepository;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockReadStream extends AbstractEventBusReadStream {

  private AtomicInteger seq = new AtomicInteger();

  public BlockReadStream(EventQueue queue, EventConsumerRepository consumerDao) {
    super(queue, consumerDao);
  }

  @Override
  public List<Event> poll() {
    int min = seq.get();
    int max = min + 100;
    List<Event> events = new ArrayList<>();
    for (int i = min; i < max; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar", "deviceId", new Random().nextInt(10)));
      Event event = Event.create("DeviceControlEvent", message, 1);
      events.add(event);
    }
    return events;
  }

  @Override
  public void start() {

  }

  @Override
  public void close() {

  }
}
