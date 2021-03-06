package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Event;
import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.EventConsumerRepository;
import com.github.edgar615.message.utils.EventQueue;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockReadStream extends AbstractEventBusReadStream {

  private AtomicInteger seq = new AtomicInteger();

  public BlockReadStream(EventQueue queue, EventConsumerRepository consumerRepository) {
    super(queue, consumerRepository);
  }

  @Override
  public List<Message> poll() {
    int min = seq.get();
    int max = min + 100;
    List<Message> messages = new ArrayList<>();
    for (int i = min; i < max; i++) {
      Event event = Event.create("" + i, ImmutableMap.of("foo", "bar", "deviceId", new Random().nextInt(10)));
      Message message = Message.create("DeviceControlEvent", event, 1);
      messages.add(message);
    }
    return messages;
  }

  @Override
  public void start() {

  }

  @Override
  public void close() {

  }
}
