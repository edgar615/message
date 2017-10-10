package com.github.edgar615.util.eventbus;

import com.google.common.collect.ImmutableMap;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.event.Message;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class SendEventTest {

  @Test
  public void testSend() {
    ProducerOptions options = new ProducerOptions();
    EventProducer producer = new BlockProducer(options, 1);
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      producer.send(event);
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testStorage() {
    ProducerOptions options = new ProducerOptions();
    MockProducerStorage storage = new MockProducerStorage();
    options.setProducerStorage(storage);
    BlockProducer producer = new BlockProducer(options, 0);
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("sms", message);
        producer.send(event);
      } else {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        producer.send(event);
      }
    }

    Awaitility.await().until(() -> storage.getEvents().size() == 9);
    long count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("1"))
            .count();
    Assert.assertEquals(9, count);
  }

  @Test
  public void testMark() {
    ProducerOptions options = new ProducerOptions();
    MockProducerStorage storage = new MockProducerStorage();
    options.setProducerStorage(storage);
    RoundRobinProducer producer = new RoundRobinProducer(options, 0);

    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("sms", message);
        producer.send(event);
      } else {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        producer.send(event);
      }
    }
    Awaitility.await().until(() -> storage.getEvents().size() == 9);
    long count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("1"))
            .count();
    Assert.assertEquals(5, count);

    count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("2"))
            .count();
    Assert.assertEquals(4, count);
  }

  @Test
  public void testExpire() {
    ProducerOptions options = new ProducerOptions();
    MockProducerStorage storage = new MockProducerStorage();
    options.setProducerStorage(storage);
    BlockProducer producer = new BlockProducer(options, 0);
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message, 1);
        try {
          TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        producer.send(event);
      } else {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        producer.send(event);
      }
    }
    Awaitility.await().until(() -> storage.getEvents().size() == 10);
    long count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("1"))
            .count();
    Assert.assertEquals(5, count);

    count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("2"))
            .count();
    Assert.assertEquals(0, count);
    count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("3"))
            .count();
    Assert.assertEquals(5, count);
  }

  @Test
  public void testPending() {
    ProducerOptions options = new ProducerOptions();
    MockProducerStorage storage = new MockProducerStorage();
    options.setProducerStorage(storage);
    options.setFetchPendingPeriod(3000);
    BlockProducer producer = new BlockProducer(options, 3);

    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      event.head().addExt("status", "0");
      storage.addEvent(event);
    }
    Assert.assertEquals(10, storage.getEvents().size());
    try {
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(20, storage.getEvents().size());
  }
}
