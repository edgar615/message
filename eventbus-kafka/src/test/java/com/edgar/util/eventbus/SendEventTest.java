package com.edgar.util.eventbus;

import com.google.common.collect.ImmutableMap;

import com.edgar.util.event.Event;
import com.edgar.util.event.Message;
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

//  @Test
//  public void overflowShouldReturnFalse() {
//    EventbusOptions eventbusOptions = new EventbusOptions()
//            .setMaxSendSize(3);
//    eventbusOptions.setSendBackend(new BlockSendBackend(3));
//    Eventbus eventbus = Eventbus.create(eventbusOptions);
//    List<Boolean> results = new ArrayList<>();
//    for (int i = 0; i < 10; i++) {
//      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//      Event event = Event.create("test", message);
//      results.add(eventbus.send(event));
//    }
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    System.out.println(results);
//    long successCount = results.stream().filter(r -> r).count();
//    Assert.assertTrue(successCount < 10);
//  }
//
//  @Test
//  public void testSend() {
//    EventbusOptions eventbusOptions = new EventbusOptions();
//    Eventbus eventbus = Eventbus.create(eventbusOptions);
//    for (int i = 0; i < 10; i++) {
//      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//      Event event = Event.create("test", message);
//      eventbus.send(event);
//    }
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void testStorage() {
//    EventbusOptions eventbusOptions = new EventbusOptions();
//    eventbusOptions.setSendBackend(new BlockSendBackend(0));
//    MockSendStorage storage = new MockSendStorage();
//    eventbusOptions.setSendStorage(storage);
//    Eventbus eventbus = Eventbus.create(eventbusOptions);
//    for (int i = 0; i < 10; i++) {
//      if (i == 5) {
//        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//        Event event = Event.create("sms", message);
//        eventbus.send(event);
//      } else {
//        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//        Event event = Event.create("test", message);
//        eventbus.send(event);
//      }
//    }
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    Assert.assertEquals(9, storage.getEvents().size());
//    long count = storage.getEvents().stream()
//            .filter(e -> e.head().ext("status").equalsIgnoreCase("1"))
//            .count();
//    Assert.assertEquals(9, count);
//  }
//
//  @Test
//  public void testMark() {
//    EventbusOptions eventbusOptions = new EventbusOptions();
//    eventbusOptions.setSendBackend(new RoundRobinSendBackend(0));
//
//    MockSendStorage storage = new MockSendStorage();
//    eventbusOptions.setSendStorage(storage);
//    Eventbus eventbus = Eventbus.create(eventbusOptions);
//    for (int i = 0; i < 10; i++) {
//      if (i == 5) {
//        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//        Event event = Event.create("sms", message);
//        eventbus.send(event);
//      } else {
//        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//        Event event = Event.create("test", message);
//        eventbus.send(event);
//      }
//    }
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    Assert.assertEquals(9, storage.getEvents().size());
//    long count = storage.getEvents().stream()
//            .filter(e -> e.head().ext("status").equalsIgnoreCase("1"))
//            .count();
//    Assert.assertEquals(5, count);
//
//    count = storage.getEvents().stream()
//            .filter(e -> e.head().ext("status").equalsIgnoreCase("2"))
//            .count();
//    Assert.assertEquals(4, count);
//  }
//
//  @Test
//  public void testExpire() {
//    EventbusOptions eventbusOptions = new EventbusOptions();
//    eventbusOptions.setGroup(group);
//    eventbusOptions.setId(clientId);
//    eventbusOptions.setSendBackend(new BlockSendBackend(0));
//
//    MockSendStorage storage = new MockSendStorage();
//    eventbusOptions.setSendStorage(storage);
//    Eventbus eventbus = Eventbus.create(eventbusOptions);
//    for (int i = 0; i < 10; i++) {
//      if (i % 2 == 0) {
//        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//        Event event = Event.create("sms", message, 1);
//        try {
//          TimeUnit.SECONDS.sleep(2);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//        eventbus.send(event);
//      } else {
//        Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//        Event event = Event.create("test", message);
//        eventbus.send(event);
//      }
//    }
//    try {
//      TimeUnit.SECONDS.sleep(3);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    Assert.assertEquals(5, storage.getEvents().size());
//    long count = storage.getEvents().stream()
//            .filter(e -> e.head().ext("status").equalsIgnoreCase("1"))
//            .count();
//    Assert.assertEquals(5, count);
//
//    count = storage.getEvents().stream()
//            .filter(e -> e.head().ext("status").equalsIgnoreCase("2"))
//            .count();
//    Assert.assertEquals(0, count);
//  }
//
//  @Test
//  public void testPending() {
//    EventbusOptions eventbusOptions = new EventbusOptions();
//    eventbusOptions.setGroup(group);
//    eventbusOptions.setId(clientId);
//    eventbusOptions.setFetchPendingPeriod(3000);
//    eventbusOptions.setSendBackend(new BlockSendBackend(0));
//
//    MockSendStorage storage = new MockSendStorage();
//    eventbusOptions.setSendStorage(storage);
//    Eventbus eventbus = Eventbus.create(eventbusOptions);
//    for (int i = 0; i < 10; i++) {
//      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
//      Event event = Event.create("test", message);
//      event.head().addExt("status", "0");
//      storage.addEvent(event);
//    }
//    try {
//      TimeUnit.SECONDS.sleep(7);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    Assert.assertEquals(10, storage.getEvents().size());
//    long count = storage.getEvents().stream()
//            .filter(e -> e.head().ext("status").equalsIgnoreCase("1"))
//            .count();
//    Assert.assertEquals(10, count);
//
//    count = storage.getEvents().stream()
//            .filter(e -> e.head().ext("status").equalsIgnoreCase("2"))
//            .count();
//    Assert.assertEquals(0, count);
//  }
}
