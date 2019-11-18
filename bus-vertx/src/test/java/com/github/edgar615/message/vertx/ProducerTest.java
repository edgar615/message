package com.github.edgar615.message.vertx;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.core.Event;
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
    VertxMessageProducer producer = VertxMessageProducer.create(vertx, writeStream);
    producer.start();
    AtomicInteger complete = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      Map<String, Object> content = new HashMap<>();
      content.put("foo", "bar");
      Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
      Message message = Message.create("test", event);
      producer.send(message, ar -> {
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
    VertxMessageProducer producer = VertxMessageProducer.create(vertx, writeStream, producerDao);
    producer.start();
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
        Message message = Message.create("sms", event);
        producer.send(message, ar -> {

        });
      } else {
        Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
        Message message = Message.create("test", event);
        producer.save(message, ar -> {

        });
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
    VertxMessageProducerScheduler eventBusProducerScheduler = VertxMessageProducerScheduler
        .create(vertx, producerDao, writeStream, 3000);
    eventBusProducerScheduler.start();
    VertxMessageProducer producer = VertxMessageProducer.create(vertx, writeStream, producerDao);
    producer.start();
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
        Message message = Message.create("sms", event);
        producer.send(message, ar -> {});
      } else {
        Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
        Message message = Message.create("test", event);
        producer.save(message, ar -> {});
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
}
