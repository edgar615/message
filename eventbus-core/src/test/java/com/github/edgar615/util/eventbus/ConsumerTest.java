package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.event.Message;
import com.google.common.collect.ImmutableMap;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2018/5/12.
 *
 * @author Edgar  Date 2018/5/12
 */
public class ConsumerTest {

  @Test
  public void testConsumer() throws InterruptedException {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(5)
            .setMaxQuota(10);
    AtomicInteger count = new AtomicInteger();
    MockConsumer mockConsumer = new MockConsumer(options);
    mockConsumer.consumer((t, r) -> true, e -> {
      System.out.println(Thread.currentThread() + ":" + e.action().resource());
      count.incrementAndGet();
    });
    for (int i = 0; i < 100; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("DeviceControlEvent_1_3", message, 1);
      mockConsumer.pollEvent(event);
    }
    TimeUnit.SECONDS.sleep(3);
    Awaitility.await().until(() -> count.get() == 100);
  }

  @Test
  public void testStorage() throws InterruptedException {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(5)
            .setMaxQuota(10);
    AtomicInteger count = new AtomicInteger();
    MockConsumerStorage storage = new MockConsumerStorage();
    MockConsumer mockConsumer = new MockConsumer(options, storage, null,
                                                 e -> Integer.parseInt(e.action().resource()) % 10
                                                      == 0);
    mockConsumer.consumer((t, r) -> true, e -> {
      System.out.println(Thread.currentThread() + ":" + e.action().resource());
      count.incrementAndGet();
      if (Integer.parseInt(e.action().resource()) % 5 == 0) {
        throw new RuntimeException("error");
      }
    });
    for (int i = 0; i < 100; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("DeviceControlEvent_1_3", message, 1);
      mockConsumer.pollEvent(event);
    }
    Awaitility.await().until(() -> count.get() == 90);
    TimeUnit.SECONDS.sleep(1);
    Assert.assertEquals(90, count.get());
    long failedCount = storage.getEvents().stream()
            .filter(e -> "2".equals(e.head().ext("status")))
            .count();
    Assert.assertEquals(10, failedCount);
    long blackCount = storage.getEvents().stream()
            .filter(e -> "3".equals(e.head().ext("status")))
            .count();
    Assert.assertEquals(10, blackCount);
  }

  @Test
  public void testBlackList() throws InterruptedException {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(5)
            .setMaxQuota(10);
    AtomicInteger count = new AtomicInteger();
    MockConsumer mockConsumer = new MockConsumer(options, null, null,
                                                 e -> Integer.parseInt(e.action().resource()) % 5
                                                      == 0);
    mockConsumer.consumer((t, r) -> true, e -> {
      System.out.println(Thread.currentThread() + ":" + e.action().resource());
      count.incrementAndGet();
    });
    for (int i = 0; i < 100; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("DeviceControlEvent_1_3", message, 1);
      mockConsumer.pollEvent(event);
    }
    Awaitility.await().until(() -> count.get() == 80);
    TimeUnit.SECONDS.sleep(1);
    Assert.assertEquals(80, count.get());
  }

  @Test
  public void testSeq() throws InterruptedException {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(5)
            .setMaxQuota(5);
    AtomicInteger count = new AtomicInteger();
    MockConsumer mockConsumer = new MockConsumer(options, null, e -> e.action().resource(),null);
    mockConsumer.consumer((t, r) -> true, e -> {
      System.out.println(Thread.currentThread() + ":" + e.action().resource());
      try {
        TimeUnit.MILLISECONDS.sleep(500);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      count.incrementAndGet();
    });
    for (int i = 0; i < 100; i++) {
      Message message = Message.create("" + (i % 10), ImmutableMap.of("foo", "bar"));
      Event event = Event.create("DeviceControlEvent_1_3", message, 1);
      mockConsumer.pollEvent(event);
    }
    TimeUnit.SECONDS.sleep(10);
    Awaitility.await().until(() -> count.get() == 100);
    Assert.assertEquals(100, count.get());
  }
}
