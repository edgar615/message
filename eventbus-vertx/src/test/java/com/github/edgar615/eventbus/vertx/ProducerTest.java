package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ProducerTest {

  private Vertx vertx;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
  }

  @Test
  public void testSend() {
    BlockWriteStream writeStream = new BlockWriteStream(1);
    VertxEventBusProducer producer = VertxEventBusProducer.create(vertx, writeStream);
    producer.start();
    AtomicInteger complete = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      Map<String, Object> content = new HashMap<>();
      content.put("foo", "bar");
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      producer.send(event, ar -> {
        complete.incrementAndGet();
      });
    }
    Awaitility.await().until(() -> complete.get() == 10);
    producer.close();
  }

  @Test
  public void testStorage() {
    MockProducerRepository producerDao = new MockProducerRepository();
    RoundRobinWriteStream writeStream = new RoundRobinWriteStream();
    VertxEventBusProducer producer = VertxEventBusProducer.create(vertx, writeStream, producerDao);
    producer.start();
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("sms", message);
        producer.send(event, ar -> {

        });
      } else {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        producer.save(event, ar -> {

        });
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
    Assert.assertEquals(0, count);

    count = producerDao.getEvents().stream()
        .filter(e -> "3".equalsIgnoreCase(e.head().ext("state")))
        .count();
    Assert.assertEquals(0, count);
    producer.close();
  }

  @Test
  public void testScheduler() {
    MockProducerRepository producerDao = new MockProducerRepository();
    RoundRobinWriteStream writeStream = new RoundRobinWriteStream();
    VertxEventBusProducerScheduler eventBusProducerScheduler = VertxEventBusProducerScheduler.create(vertx, producerDao, writeStream, 3000);
    eventBusProducerScheduler.start();
    VertxEventBusProducer producer = VertxEventBusProducer.create(vertx, writeStream, producerDao);
    producer.start();
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("sms", message);
        producer.send(event, ar -> {});
      } else {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        producer.save(event, ar -> {});
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
    eventBusProducerScheduler.close();
  }
}
