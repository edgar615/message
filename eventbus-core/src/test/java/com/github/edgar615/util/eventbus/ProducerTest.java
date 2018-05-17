package com.github.edgar615.util.eventbus;

import com.google.common.collect.ImmutableMap;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.event.Message;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class ProducerTest {

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
    producer.close();
  }

  @Test
  public void testMaxQuota() {
    ProducerOptions options = new ProducerOptions()
            .setMaxQuota(5);
    EventProducer producer = new BlockProducer(options, 5);
    AtomicInteger succeed = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();
    for (int i = 0; i < 11; i++) {
      try {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        producer.send(event);
        succeed.incrementAndGet();
      } catch (Exception e) {
        e.printStackTrace();
        failed.incrementAndGet();
      }
    }
    Assert.assertTrue(succeed.get() < 11);
    producer.close();
  }

  @Test
  public void testMaxQuotaWithPersist() {
    MockProducerStorage storage = new MockProducerStorage();
    ProducerOptions options = new ProducerOptions()
            .setMaxQuota(5);
    EventProducer producer = new BlockProducer(options, storage, 5);
    AtomicInteger succeed = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();
    for (int i = 0; i < 11; i++) {
      try {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        producer.send(event);
        succeed.incrementAndGet();
      } catch (Exception e) {
        e.printStackTrace();
        failed.incrementAndGet();
      }
    }
    Assert.assertEquals(succeed.get(), 11);
    producer.close();
  }

  @Test
  public void testStorage() {
    ProducerOptions options = new ProducerOptions().setFetchPendingPeriod(10);
    MockProducerStorage storage = new MockProducerStorage();
    BlockProducer producer = new BlockProducer(options, storage, 0);
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
            .filter(e -> "1".equalsIgnoreCase(e.head().ext("status")))
            .count();
    Assert.assertEquals(9, count);
    producer.close();
  }

  @Test
  public void testMark() {
    ProducerOptions options = new ProducerOptions().setFetchPendingPeriod(100);
    MockProducerStorage storage = new MockProducerStorage();
    RoundRobinProducer producer = new RoundRobinProducer(options, storage, 0);
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
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    long count = storage.getEvents().stream()
            .filter(e -> "1".equalsIgnoreCase(e.head().ext("status")))
            .count();
    Assert.assertEquals(4, count);

    count = storage.getEvents().stream()
            .filter(e -> "2".equalsIgnoreCase(e.head().ext("status")))
            .count();
    Assert.assertEquals(5, count);
    producer.close();
  }

  @Test
  public void testExpire() {
    ProducerOptions options = new ProducerOptions().setFetchPendingPeriod(10);
    MockProducerStorage storage = new MockProducerStorage();
    BlockProducer producer = new BlockProducer(options, storage, 0);
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
//    Awaitility.await().until(() -> storage.getEvents().size() == 10);
    Awaitility.await().until(() -> {
      long c1 = storage.getEvents().stream()
              .filter(e ->"1".equalsIgnoreCase(e.head().ext("status"))).count();
      long c2 = storage.getEvents().stream()
              .filter(e ->"2".equalsIgnoreCase(e.head().ext("status"))).count();
      long c3 = storage.getEvents().stream()
              .filter(e ->"3".equalsIgnoreCase(e.head().ext("status"))).count();
      return c1 + c2 + c3 == 10;
    });
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
    producer.close();
  }

  @Test
  public void testPending() {
    ProducerOptions options = new ProducerOptions();
    MockProducerStorage storage = new MockProducerStorage();
    options.setFetchPendingPeriod(3000);
    BlockProducer producer = new BlockProducer(options, storage, 0);
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      storage.addEvent(event);
    }
    Assert.assertEquals(10, storage.getEvents().size());
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      storage.addEvent(event);
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(20, storage.getEvents().size());
    Assert.assertFalse(
            storage.getEvents().stream().anyMatch(e -> "0".equals(e.head().ext("status"))));
    producer.close();
    System.out.println(storage.getPendCount());
//    Assert.assertTrue(storage.getPendCount() > 10);
  }

  @Test
  public void testPendingDelay() {
    ProducerOptions options = new ProducerOptions()
            .setMaxQuota(1);
    MockProducerStorage storage = new MockProducerStorage();
    options.setFetchPendingPeriod(3000);
    BlockProducer producer = new BlockProducer(options, storage, 3);
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      storage.addEvent(event);
    }
    Assert.assertEquals(10, storage.getEvents().size());
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      storage.addEvent(event);
    }
    Assert.assertEquals(20, storage.getEvents().size());
    Assert.assertTrue(
            storage.getEvents().stream().anyMatch(e -> !e.head().ext().containsKey("status")));
    producer.close();
    System.out.println(storage.getPendCount());
//    Assert.assertEquals(1, storage.getPendCount());
  }
}
