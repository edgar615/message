package com.edgar.util.eventbus.kafka;

import com.google.common.collect.ImmutableMap;

import com.edgar.util.event.Event;
import com.edgar.util.event.Message;
import com.edgar.util.eventbus.Eventbus;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class SendEventTest extends EventbusTest {

  @Test
  public void overflowShouldReturnFalse() {
    KafkaEventbusOptions kafkaEventbusOptions = new KafkaEventbusOptions();
    kafkaEventbusOptions.setGroup(group);
    kafkaEventbusOptions.setId(clientId);
    kafkaEventbusOptions.setMaxSendSize(3);
    kafkaEventbusOptions.getPrducerOptions().setServers(server);
    kafkaEventbusOptions.setSendBackend(new BlockSendBackend(3));
    Eventbus eventbus = Eventbus.create(kafkaEventbusOptions);
    List<Boolean> results = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      results.add(eventbus.send(event));
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println(results);
    long successCount = results.stream().filter(r -> r).count();
    Assert.assertTrue(successCount < 10);
  }

  @Test
  public void testSend() {
    KafkaEventbusOptions kafkaEventbusOptions = new KafkaEventbusOptions();
    kafkaEventbusOptions.setGroup(group);
    kafkaEventbusOptions.setId(clientId);
    kafkaEventbusOptions.getPrducerOptions().setServers(server);
    kafkaEventbusOptions.setSendBackend(new BlockSendBackend(0));
    Eventbus eventbus = Eventbus.create(kafkaEventbusOptions);
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      eventbus.send(event);
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testStorage() {
    KafkaEventbusOptions kafkaEventbusOptions = new KafkaEventbusOptions();
    kafkaEventbusOptions.setGroup(group);
    kafkaEventbusOptions.setId(clientId);
    kafkaEventbusOptions.getPrducerOptions().setServers(server);
    kafkaEventbusOptions.setSendBackend(new BlockSendBackend(0));

    MockSendStorage storage = new MockSendStorage();
    kafkaEventbusOptions.setSendStorage(storage);
    Eventbus eventbus = Eventbus.create(kafkaEventbusOptions);
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("sms", message);
        eventbus.send(event);
      } else {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        eventbus.send(event);
      }
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(9, storage.getEvents().size());
    long count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("1"))
            .count();
    Assert.assertEquals(9, count);
  }

  @Test
  public void testMark() {
    KafkaEventbusOptions kafkaEventbusOptions = new KafkaEventbusOptions();
    kafkaEventbusOptions.setGroup(group);
    kafkaEventbusOptions.setId(clientId);
    kafkaEventbusOptions.getPrducerOptions().setServers(server);
    kafkaEventbusOptions.setSendBackend(new RoundRobinSendBackend(0));

    MockSendStorage storage = new MockSendStorage();
    kafkaEventbusOptions.setSendStorage(storage);
    Eventbus eventbus = Eventbus.create(kafkaEventbusOptions);
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("sms", message);
        eventbus.send(event);
      } else {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        eventbus.send(event);
      }
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(9, storage.getEvents().size());
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
    KafkaEventbusOptions kafkaEventbusOptions = new KafkaEventbusOptions();
    kafkaEventbusOptions.setGroup(group);
    kafkaEventbusOptions.setId(clientId);
    kafkaEventbusOptions.getPrducerOptions().setServers(server);
    kafkaEventbusOptions.setSendBackend(new BlockSendBackend(0));

    MockSendStorage storage = new MockSendStorage();
    kafkaEventbusOptions.setSendStorage(storage);
    Eventbus eventbus = Eventbus.create(kafkaEventbusOptions);
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("sms", message, 1);
        try {
          TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        eventbus.send(event);
      } else {
        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
        Event event = Event.create("test", message);
        eventbus.send(event);
      }
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(5, storage.getEvents().size());
    long count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("1"))
            .count();
    Assert.assertEquals(5, count);

    count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("2"))
            .count();
    Assert.assertEquals(0, count);
  }

  @Test
  public void testPending() {
    KafkaEventbusOptions kafkaEventbusOptions = new KafkaEventbusOptions();
    kafkaEventbusOptions.setGroup(group);
    kafkaEventbusOptions.setId(clientId);
    kafkaEventbusOptions.setFetchPendingPeriod(3000);
    kafkaEventbusOptions.getPrducerOptions().setServers(server);
    kafkaEventbusOptions.setSendBackend(new BlockSendBackend(0));

    MockSendStorage storage = new MockSendStorage();
    kafkaEventbusOptions.setSendStorage(storage);
    Eventbus eventbus = Eventbus.create(kafkaEventbusOptions);
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      event.head().addExt("status", "0");
      storage.addEvent(event);
    }
    try {
      TimeUnit.SECONDS.sleep(7);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(10, storage.getEvents().size());
    long count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("1"))
            .count();
    Assert.assertEquals(10, count);

    count = storage.getEvents().stream()
            .filter(e -> e.head().ext("status").equalsIgnoreCase("2"))
            .count();
    Assert.assertEquals(0, count);
  }
}
