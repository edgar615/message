package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.MessageProducerRepository;
import com.github.edgar615.message.repository.SendMessageState;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class MockProducerRepository implements MessageProducerRepository {

  private final List<Message> messages = new ArrayList<>();

  private AtomicInteger pendCount = new AtomicInteger();

  public List<Message> getMessages() {
    return ImmutableList.copyOf(messages);
  }

  public MockProducerRepository addEvent(Message message) {
    messages.add(message);
    return this;
  }

  public int getPendCount() {
    return pendCount.get();
  }

  @Override
  public void insert(Message message) {
    messages.add(message);
  }

  @Override
  public List<Message> waitingForSend() {
    pendCount.incrementAndGet();
    List<Message> plist = messages.stream().filter(e -> !e.header().ext().containsKey("state"))
        .collect(Collectors.toList());
    return new ArrayList<>(plist);
  }

  @Override
  public List<Message> waitingForSend(int fetchCount) {
    return null;
  }

  @Override
  public void mark(String eventId, SendMessageState state) {
    messages.stream().filter(e -> e.header().id().equalsIgnoreCase(eventId))
        .forEach(e -> e.header().addExt("state", state.value() + ""));
  }
}
