package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class ProducerTest {

  @Test
  public void testSend() {
    BlockWriteStream writeStream = new BlockWriteStream(1);
    EventProducer producer = new EventProducerImpl(new ProducerOptions(), writeStream);
    producer.start();
    AtomicInteger complete = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      Map<String, Object> content = new HashMap<>();
      content.put("foo", "bar");
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      CompletableFuture<Event> future = producer.send(event);
      future.thenAccept(e -> complete.incrementAndGet());
    }
    Awaitility.await().until(() -> complete.get() == 10);
    producer.close();
  }


  @Test
  public void testStorage() {
    MockProducerDao producerDao = new MockProducerDao();
    RoundRobinWriteStream writeStream = new RoundRobinWriteStream();
    EventBusScheduler eventBusScheduler = new EventBusSchedulerImpl(producerDao, writeStream, 3000);
    EventProducer producer = new EventProducerImpl(new ProducerOptions(), writeStream, producerDao, eventBusScheduler);
    producer.start();
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("sms", message);
        producer.send(event);
      } else {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        producer.save(event);
      }
    }

    Awaitility.await().until(() -> producerDao.getEvents().size() == 9);
    long count = producerDao.getEvents().stream()
        .filter(e -> e.head().ext("state") == null)
        .count();
    Assert.assertEquals(9, count);

    try {
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    count = producerDao.getEvents().stream()
        .filter(e -> "2".equalsIgnoreCase(e.head().ext("state")))
        .count();
    Assert.assertEquals(4, count);

    count = producerDao.getEvents().stream()
        .filter(e -> "3".equalsIgnoreCase(e.head().ext("state")))
        .count();
    Assert.assertEquals(5, count);
    producer.close();
  }

//  @Test
//  public void testMaxQuota() {
//    ProducerOptions options = new ProducerOptions();
//    BlockWriteStream writeStream = new BlockWriteStream(5);
//    EventProducer producer = new EventProducerImpl(options, writeStream);
//    AtomicInteger succeed = new AtomicInteger();
//    AtomicInteger failed = new AtomicInteger();
//    for (int i = 0; i < 11; i++) {
//      try {
//        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//        Event event = Event.create("test", message);
//        producer.send(event);
//        succeed.incrementAndGet();
//      } catch (Exception e) {
//        e.printStackTrace();
//        failed.incrementAndGet();
//      }
//    }
//    Assert.assertTrue(succeed.get() < 11);
//    producer.close();
//  }
//
//  @Test
//  public void testMaxQuotaWithPersist() {
//    MockProducerDao storage = new MockProducerDao();
//    ProducerOptions options = new ProducerOptions()
//            .setMaxQuota(5);
//    EventProducer producer = new BlockWriteStream(options, storage, 5);
//    AtomicInteger succeed = new AtomicInteger();
//    AtomicInteger failed = new AtomicInteger();
//    for (int i = 0; i < 11; i++) {
//      try {
//        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//        Event event = Event.create("test", message);
//        producer.send(event);
//        succeed.incrementAndGet();
//      } catch (Exception e) {
//        e.printStackTrace();
//        failed.incrementAndGet();
//      }
//    }
//    Assert.assertEquals(succeed.get(), 11);
//    producer.close();
//  }
//  @Test
//  public void testExpire() {
//    ProducerOptions options = new ProducerOptions().setFetchPendingPeriod(10);
//    MockProducerDao storage = new MockProducerDao();
//    BlockWriteStream producer = new BlockWriteStream(options, storage, 0);
//    for (int i = 0; i < 10; i++) {
//      if (i % 2 == 0) {
//        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//        Event event = Event.create("test", message, 1);
//        try {
//          TimeUnit.SECONDS.sleep(2);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//        producer.send(event);
//      } else {
//        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//        Event event = Event.create("test", message);
//        producer.send(event);
//      }
//    }
////    Awaitility.await().until(() -> storage.getEvents().size() == 10);
//    Awaitility.await().until(() -> {
//      long c1 = storage.getEvents().stream()
//              .filter(e ->"1".equalsIgnoreCase(e.head().ext("state"))).count();
//      long c2 = storage.getEvents().stream()
//              .filter(e ->"2".equalsIgnoreCase(e.head().ext("state"))).count();
//      long c3 = storage.getEvents().stream()
//              .filter(e ->"3".equalsIgnoreCase(e.head().ext("state"))).count();
//      return c1 + c2 + c3 == 10;
//    });
//    long count = storage.getEvents().stream()
//            .filter(e -> e.head().ext("state").equalsIgnoreCase("1"))
//            .count();
//    Assert.assertEquals(5, count);
//
//    count = storage.getEvents().stream()
//            .filter(e -> e.head().ext("state").equalsIgnoreCase("2"))
//            .count();
//    Assert.assertEquals(0, count);
//    count = storage.getEvents().stream()
//            .filter(e -> e.head().ext("state").equalsIgnoreCase("3"))
//            .count();
//    Assert.assertEquals(5, count);
//    producer.close();
//  }
//
//  @Test
//  public void testPending() {
//    ProducerOptions options = new ProducerOptions();
//    MockProducerDao storage = new MockProducerDao();
//    options.setFetchPendingPeriod(3000);
//    BlockWriteStream producer = new BlockWriteStream(options, storage, 0);
//    for (int i = 0; i < 10; i++) {
//      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//      Event event = Event.create("test", message);
//      storage.addEvent(event);
//    }
//    Assert.assertEquals(10, storage.getEvents().size());
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    for (int i = 0; i < 10; i++) {
//      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//      Event event = Event.create("test", message);
//      storage.addEvent(event);
//    }
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    Assert.assertEquals(20, storage.getEvents().size());
//    Assert.assertFalse(
//            storage.getEvents().stream().anyMatch(e -> "0".equals(e.head().ext("state"))));
//    producer.close();
//    System.out.println(storage.getPendCount());
////    Assert.assertTrue(storage.getPendCount() > 10);
//  }
//
//  @Test
//  public void testPendingDelay() {
//    ProducerOptions options = new ProducerOptions()
//            .setMaxQuota(1);
//    MockProducerDao storage = new MockProducerDao();
//    options.setFetchPendingPeriod(3000);
//    BlockWriteStream producer = new BlockWriteStream(options, storage, 3);
//    for (int i = 0; i < 10; i++) {
//      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//      Event event = Event.create("test", message);
//      storage.addEvent(event);
//    }
//    Assert.assertEquals(10, storage.getEvents().size());
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    for (int i = 0; i < 10; i++) {
//      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//      Event event = Event.create("test", message);
//      storage.addEvent(event);
//    }
//    Assert.assertEquals(20, storage.getEvents().size());
//    Assert.assertTrue(
//            storage.getEvents().stream().anyMatch(e -> !e.head().ext().containsKey("state")));
//    producer.close();
//    System.out.println(storage.getPendCount());
////    Assert.assertEquals(1, storage.getPendCount());
//  }
}
