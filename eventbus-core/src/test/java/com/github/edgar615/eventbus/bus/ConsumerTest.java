package com.github.edgar615.eventbus.bus;

import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.github.edgar615.eventbus.dao.ConsumeEventState;
import com.github.edgar615.eventbus.dao.EventConsumerDao;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.github.edgar615.eventbus.utils.DefaultEventQueue;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.NamedThreadFactory;
import com.github.edgar615.eventbus.utils.SequentialEventQueue;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Edgar on 2018/5/12.
 *
 * @author Edgar  Date 2018/5/12
 */
public class ConsumerTest {

  @Test
  public void testConsumer() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    EventQueue eventQueue = new DefaultEventQueue(1000);
    EventConsumerDao eventConsumerDao = new MockConsumerDao();
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory.create("consumer-scheduler"));
    AtomicInteger count = new AtomicInteger();
    EventBusConsumer eventBusConsumer = new EventBusConsumerImpl(options, eventQueue, eventConsumerDao, scheduledExecutor);
    eventBusConsumer.consumer(null, null, e -> {
      count.incrementAndGet();
    });
    ((EventBusConsumerImpl) eventBusConsumer).start();
    EventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerDao);
    ((BlockReadStream) readStream).pollAndEnqueue();

    Awaitility.await().until(() -> count.get() == 100);
    eventBusConsumer.close();
    readStream.close();
  }

  @Test
  public void testWriteDb() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    EventQueue eventQueue = new DefaultEventQueue(1000);
    MockConsumerDao eventConsumerDao = new MockConsumerDao();
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory.create("consumer-scheduler"));
    AtomicInteger count = new AtomicInteger();
    EventBusConsumer eventBusConsumer = new EventBusConsumerImpl(options, eventQueue, eventConsumerDao, scheduledExecutor);
    eventBusConsumer.consumer(null, null, e -> {
      int seq = count.incrementAndGet();
      if (seq % 2 == 0) {
        throw new RuntimeException();
      }
    });
    ((EventBusConsumerImpl) eventBusConsumer).start();
    EventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerDao);
    ((BlockReadStream) readStream).pollAndEnqueue();

    Awaitility.await().until(() -> count.get() == 100);
    long successCount = eventConsumerDao.events().stream()
        .filter(e -> e.head().ext("state") != null)
        .filter(e -> e.head().ext("state").equals(String.valueOf(ConsumeEventState.SUCCEED.value())))
        .count();
    Assert.assertEquals(successCount, 50);
    long failedCount = eventConsumerDao.events().stream()
        .filter(e -> e.head().ext("state") != null)
        .filter(e -> e.head().ext("state").equals(String.valueOf(ConsumeEventState.SUCCEED.value())))
        .count();
    Assert.assertEquals(failedCount, 50);
    eventBusConsumer.close();
    readStream.close();
  }

  @Test
  public void testSeq() {

    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10)
        .setBlockedCheckerMs(300);
    SequentialEventQueue eventQueue = new SequentialEventQueue(e -> {
      Message message = (Message) e.action();
      return message.content().get("deviceId").toString();
    },
        1000);
    EventConsumerDao eventConsumerDao = new MockConsumerDao();
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory.create("consumer-scheduler"));
    AtomicInteger count = new AtomicInteger();
    List<Integer> zeroList = new ArrayList<>();
    List<Integer> fiveList = new ArrayList<>();
    EventBusConsumer eventBusConsumer = new EventBusConsumerImpl(options, eventQueue, eventConsumerDao, scheduledExecutor);
    eventBusConsumer.consumer(null, null, e -> {
      try {
        TimeUnit.MILLISECONDS.sleep(500);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      Message message = (Message) e.action();
      Integer deviceId = (Integer) message.content().get("deviceId");
      if (deviceId == 0) {
        zeroList.add(Integer.parseInt(e.action().resource()));
      }
      if (deviceId == 5) {
        fiveList.add(Integer.parseInt(e.action().resource()));
      }
      count.incrementAndGet();
    });
    ((EventBusConsumerImpl) eventBusConsumer).start();
    EventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerDao);
    ((BlockReadStream) readStream).pollAndEnqueue();

    try {
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Awaitility.await().until(() -> count.get() == 100);
    for (int i = 0; i < zeroList.size() - 1;i ++) {
      Assert.assertTrue(zeroList.get(i) < zeroList.get(i + 1));
    }
    for (int i = 0; i < fiveList.size() - 1;i ++) {
      Assert.assertTrue(fiveList.get(i) < fiveList.get(i + 1));
    }
    eventBusConsumer.close();
    readStream.close();
  }

  @Test
  public void testPauseAndResume() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    EventQueue eventQueue = new DefaultEventQueue(5);
    EventConsumerDao eventConsumerDao = new MockConsumerDao();
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory.create("consumer-scheduler"));
    AtomicInteger count = new AtomicInteger();
    EventBusConsumer eventBusConsumer = new EventBusConsumerImpl(options, eventQueue, eventConsumerDao, scheduledExecutor);
    eventBusConsumer.consumer(null, null, e -> {
      try {
        TimeUnit.MILLISECONDS.sleep(40);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      count.incrementAndGet();
    });
    ((EventBusConsumerImpl) eventBusConsumer).start();
    EventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerDao);
    ((BlockReadStream) readStream).pollAndEnqueue();
    Assert.assertTrue(readStream.paused());
    Awaitility.await().until(() -> count.get() == 100);

//    ((BlockReadStream) readStream).pollAndEnqueue();
//    Awaitility.await().until(() -> !readStream.paused());
//    Awaitility.await().until(() -> count.get() == 200);
    eventBusConsumer.close();
    readStream.close();
  }
}
