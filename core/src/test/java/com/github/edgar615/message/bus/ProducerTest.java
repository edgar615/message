package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Event;
import com.github.edgar615.message.core.Message;
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
    EventBusProducer producer = EventBusProducer.create(new ProducerOptions(), writeStream);
    producer.start();
    AtomicInteger complete = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      Map<String, Object> content = new HashMap<>();
      content.put("foo", "bar");
      Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
      Message message = Message.create("test", event);
      CompletableFuture<Message> future = producer.send(message);
      future.thenAccept(e -> complete.incrementAndGet());
    }
    Awaitility.await().until(() -> complete.get() == 10);
    producer.close();
  }


  @Test
  public void testStorage() {
    MockProducerRepository producerDao = new MockProducerRepository();
    RoundRobinWriteStream writeStream = new RoundRobinWriteStream();
    EventBusProducer producer = EventBusProducer
        .create(new ProducerOptions(), writeStream, producerDao);
    producer.start();
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
        Message message = Message.create("sms", event);
        producer.send(message);
      } else {
        Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
        Message message = Message.create("test", event);
        producer.save(message);
      }
    }

    Awaitility.await().until(() -> producerDao.getMessages().size() == 9);
    long count = producerDao.getMessages().stream()
        .filter(e -> e.header().ext("state") == null)
        .count();
    Assert.assertEquals(9, count);

    try {
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    count = producerDao.getMessages().stream()
        .filter(e -> "2".equalsIgnoreCase(e.header().ext("state")))
        .count();
    Assert.assertEquals(0, count);

    count = producerDao.getMessages().stream()
        .filter(e -> "3".equalsIgnoreCase(e.header().ext("state")))
        .count();
    Assert.assertEquals(0, count);
    producer.close();
  }

  @Test
  public void testScheduler() {
    MockProducerRepository producerDao = new MockProducerRepository();
    RoundRobinWriteStream writeStream = new RoundRobinWriteStream();
    EventBusProducerScheduler eventBusProducerScheduler = EventBusProducerScheduler.create(producerDao, writeStream, 3000);
    eventBusProducerScheduler.start();
    EventBusProducer producer = EventBusProducer
        .create(new ProducerOptions(), writeStream, producerDao);
    producer.start();
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
        Message message = Message.create("sms", event);
        producer.send(message);
      } else {
        Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
        Message message = Message.create("test", event);
        producer.save(message);
      }
    }

    Awaitility.await().until(() -> producerDao.getMessages().size() == 9);
    long count = producerDao.getMessages().stream()
        .filter(e -> e.header().ext("state") == null)
        .count();
    Assert.assertEquals(9, count);

    try {
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    count = producerDao.getMessages().stream()
        .filter(e -> "2".equalsIgnoreCase(e.header().ext("state")))
        .count();
    Assert.assertEquals(4, count);

    count = producerDao.getMessages().stream()
        .filter(e -> "3".equalsIgnoreCase(e.header().ext("state")))
        .count();
    Assert.assertEquals(5, count);
    producer.close();
    eventBusProducerScheduler.close();
  }

//  @Test
//  public void testMaxQuota() {
//    ProducerOptions options = new ProducerOptions();
//    BlockWriteStream writeStream = new BlockWriteStream(5);
//    EventBusProducer producer = new EventBusProducerImpl(options, writeStream);
//    AtomicInteger succeed = new AtomicInteger();
//    AtomicInteger failed = new AtomicInteger();
//    for (int i = 0; i < 11; i++) {
//      try {
//        Event message = Event.create("" + i, ImmutableMap.of("foo", "bar"));
//        Message core = Message.create("test", message);
//        producer.send(core);
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
//    MockProducerRepository storage = new MockProducerRepository();
//    ProducerOptions options = new ProducerOptions()
//            .setMaxQuota(5);
//    EventBusProducer producer = new BlockWriteStream(options, storage, 5);
//    AtomicInteger succeed = new AtomicInteger();
//    AtomicInteger failed = new AtomicInteger();
//    for (int i = 0; i < 11; i++) {
//      try {
//        Event message = Event.create("" + i, ImmutableMap.of("foo", "bar"));
//        Message core = Message.create("test", message);
//        producer.send(core);
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
//    MockProducerRepository storage = new MockProducerRepository();
//    BlockWriteStream producer = new BlockWriteStream(options, storage, 0);
//    for (int i = 0; i < 10; i++) {
//      if (i % 2 == 0) {
//        Event message = Event.create("" + i, ImmutableMap.of("foo", "bar"));
//        Message core = Message.create("test", message, 1);
//        try {
//          TimeUnit.SECONDS.sleep(2);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//        producer.send(core);
//      } else {
//        Event message = Event.create("" + i, ImmutableMap.of("foo", "bar"));
//        Message core = Message.create("test", message);
//        producer.send(core);
//      }
//    }
////    Awaitility.await().until(() -> storage.getMessages().size() == 10);
//    Awaitility.await().until(() -> {
//      long c1 = storage.getMessages().stream()
//              .filter(e ->"1".equalsIgnoreCase(e.header().ext("state"))).count();
//      long c2 = storage.getMessages().stream()
//              .filter(e ->"2".equalsIgnoreCase(e.header().ext("state"))).count();
//      long c3 = storage.getMessages().stream()
//              .filter(e ->"3".equalsIgnoreCase(e.header().ext("state"))).count();
//      return c1 + c2 + c3 == 10;
//    });
//    long count = storage.getMessages().stream()
//            .filter(e -> e.header().ext("state").equalsIgnoreCase("1"))
//            .count();
//    Assert.assertEquals(5, count);
//
//    count = storage.getMessages().stream()
//            .filter(e -> e.header().ext("state").equalsIgnoreCase("2"))
//            .count();
//    Assert.assertEquals(0, count);
//    count = storage.getMessages().stream()
//            .filter(e -> e.header().ext("state").equalsIgnoreCase("3"))
//            .count();
//    Assert.assertEquals(5, count);
//    producer.close();
//  }
//
//  @Test
//  public void testPending() {
//    ProducerOptions options = new ProducerOptions();
//    MockProducerRepository storage = new MockProducerRepository();
//    options.setFetchPendingPeriod(3000);
//    BlockWriteStream producer = new BlockWriteStream(options, storage, 0);
//    for (int i = 0; i < 10; i++) {
//      Event message = Event.create("" + i, ImmutableMap.of("foo", "bar"));
//      Message core = Message.create("test", message);
//      storage.addEvent(core);
//    }
//    Assert.assertEquals(10, storage.getMessages().size());
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    for (int i = 0; i < 10; i++) {
//      Event message = Event.create("" + i, ImmutableMap.of("foo", "bar"));
//      Message core = Message.create("test", message);
//      storage.addEvent(core);
//    }
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    Assert.assertEquals(20, storage.getMessages().size());
//    Assert.assertFalse(
//            storage.getMessages().stream().anyMatch(e -> "0".equals(e.header().ext("state"))));
//    producer.close();
//    System.out.println(storage.getPendCount());
////    Assert.assertTrue(storage.getPendCount() > 10);
//  }
//
//  @Test
//  public void testPendingDelay() {
//    ProducerOptions options = new ProducerOptions()
//            .setMaxQuota(1);
//    MockProducerRepository storage = new MockProducerRepository();
//    options.setFetchPendingPeriod(3000);
//    BlockWriteStream producer = new BlockWriteStream(options, storage, 3);
//    for (int i = 0; i < 10; i++) {
//      Event message = Event.create("" + i, ImmutableMap.of("foo", "bar"));
//      Message core = Message.create("test", message);
//      storage.addEvent(core);
//    }
//    Assert.assertEquals(10, storage.getMessages().size());
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    for (int i = 0; i < 10; i++) {
//      Event message = Event.create("" + i, ImmutableMap.of("foo", "bar"));
//      Message core = Message.create("test", message);
//      storage.addEvent(core);
//    }
//    Assert.assertEquals(20, storage.getMessages().size());
//    Assert.assertTrue(
//            storage.getMessages().stream().anyMatch(e -> !e.header().ext().containsKey("state")));
//    producer.close();
//    System.out.println(storage.getPendCount());
////    Assert.assertEquals(1, storage.getPendCount());
//  }
}
