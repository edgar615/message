package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Event;
import com.github.edgar615.message.repository.ConsumeEventState;
import com.github.edgar615.message.repository.EventConsumerRepository;
import com.github.edgar615.message.utils.DefaultEventQueue;
import com.github.edgar615.message.utils.EventQueue;
import com.github.edgar615.message.utils.SequentialEventQueue;
import java.util.ArrayList;
import java.util.List;
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

  @Before
  public void setUp() {
    HandlerRegistry.instance().unregisterAll(new HandlerKey(null, null));
  }

  @Test
  public void testConsumer() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    EventQueue eventQueue = DefaultEventQueue.create(1000);
    EventConsumerRepository eventConsumerRepository = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    EventBusConsumer eventBusConsumer = EventBusConsumer.create(options, eventQueue,
        eventConsumerRepository);
    eventBusConsumer.consumer(null, null, e -> {
      count.incrementAndGet();
    });
    ((EventBusConsumerImpl) eventBusConsumer).start();
    EventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerRepository);
    ((BlockReadStream) readStream).pollAndEnqueue();

    Awaitility.await().until(() -> count.get() == 100);
    eventBusConsumer.close();
    readStream.close();
  }

  @Test
  public void testWriteDb() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    EventQueue eventQueue = DefaultEventQueue.create(1000);
    MockConsumerRepository eventConsumerDao = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    EventBusConsumer eventBusConsumer = EventBusConsumer.create(options, eventQueue, eventConsumerDao);
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
        .filter(e -> e.header().ext("state") != null)
        .filter(e -> e.header().ext("state").equals(String.valueOf(ConsumeEventState.SUCCEED.value())))
        .count();
    Assert.assertEquals(successCount, 50);
    long failedCount = eventConsumerDao.events().stream()
        .filter(e -> e.header().ext("state") != null)
        .filter(e -> e.header().ext("state").equals(String.valueOf(ConsumeEventState.SUCCEED.value())))
        .count();
    Assert.assertEquals(failedCount, 50);
    eventBusConsumer.close();
    readStream.close();
  }

  @Test
  public void testSeq() {

    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10)
        .setBlockedCheckerMs(300);
    SequentialEventQueue eventQueue = SequentialEventQueue.create(e -> {
      Event event = (Event) e.body();
      return event.content().get("deviceId").toString();
    },
        1000);
    EventConsumerRepository eventConsumerRepository = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    List<Integer> zeroList = new ArrayList<>();
    List<Integer> fiveList = new ArrayList<>();
    EventBusConsumer eventBusConsumer = EventBusConsumer.create(options, eventQueue,
        eventConsumerRepository);
    eventBusConsumer.consumer(null, null, e -> {
      try {
        TimeUnit.MILLISECONDS.sleep(500);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      Event event = (Event) e.body();
      Integer deviceId = (Integer) event.content().get("deviceId");
      if (deviceId == 0) {
        zeroList.add(Integer.parseInt(e.body().resource()));
      }
      if (deviceId == 5) {
        fiveList.add(Integer.parseInt(e.body().resource()));
      }
      count.incrementAndGet();
    });
    eventBusConsumer.start();
    EventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerRepository);
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
    EventQueue eventQueue = DefaultEventQueue.create(5);
    EventConsumerRepository eventConsumerRepository = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    EventBusConsumer eventBusConsumer = EventBusConsumer.create(options, eventQueue,
        eventConsumerRepository);
    eventBusConsumer.consumer(null, null, e -> {
      try {
        TimeUnit.MILLISECONDS.sleep(40);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      count.incrementAndGet();
    });
    eventBusConsumer.start();
    EventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerRepository);
    ((BlockReadStream) readStream).pollAndEnqueue();
    Assert.assertTrue(readStream.paused());
    Awaitility.await().until(() -> count.get() == 100);

//    ((BlockReadStream) readStream).pollAndEnqueue();
//    Awaitility.await().until(() -> !readStream.paused());
//    Awaitility.await().until(() -> count.get() == 200);
    eventBusConsumer.close();
    readStream.close();
  }

  @Test
  public void testScheduler() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    EventQueue eventQueue = DefaultEventQueue.create(1000);
    MockConsumerRepository eventConsumerDao = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    EventBusConsumer eventBusConsumer = EventBusConsumer.create(options, eventQueue, eventConsumerDao);
    eventBusConsumer.consumer(null, null, e -> {
      int seq = count.incrementAndGet();
      if (seq % 2 == 0) {
        throw new RuntimeException();
      }
    });
    eventBusConsumer.start();
    EventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerDao);

    EventBusConsumerScheduler scheduler = EventBusConsumerScheduler.create(eventConsumerDao, eventQueue, 1000L);
    scheduler.start();

    Awaitility.await().until(() -> count.get() == 100);


    Awaitility.await().until(() -> {
      long successCount = eventConsumerDao.events().stream()
          .filter(e -> e.header().ext("state") != null)
          .filter(e -> e.header().ext("state").equals(String.valueOf(ConsumeEventState.SUCCEED.value())))
          .count();
      return successCount == 50;
    });
    Awaitility.await().until(() -> {
      long failedCount = eventConsumerDao.events().stream()
          .filter(e -> e.header().ext("state") != null)
          .filter(e -> e.header().ext("state").equals(String.valueOf(ConsumeEventState.SUCCEED.value())))
          .count();
      return failedCount == 50;
    });
    eventBusConsumer.close();
    readStream.close();
    scheduler.close();
  }
}
