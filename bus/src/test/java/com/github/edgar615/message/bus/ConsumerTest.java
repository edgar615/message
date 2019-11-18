package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Event;
import com.github.edgar615.message.repository.ConsumeMessageState;
import com.github.edgar615.message.repository.MessageConsumerRepository;
import com.github.edgar615.message.utils.DefaultMessageQueue;
import com.github.edgar615.message.utils.MessageQueue;
import com.github.edgar615.message.utils.SequentialMessageQueue;
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
    MessageQueue messageQueue = DefaultMessageQueue.create(1000);
    MessageConsumerRepository messageConsumerRepository = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    MessageConsumer messageConsumer = MessageConsumer.create(options, messageQueue,
        messageConsumerRepository);
    messageConsumer.consumer(null, null, e -> {
      count.incrementAndGet();
    });
    ((MessageConsumerImpl) messageConsumer).start();
    MessageReadStream readStream = new BlockReadStream(messageQueue, messageConsumerRepository);
    ((BlockReadStream) readStream).pollAndEnqueue();

    Awaitility.await().until(() -> count.get() == 100);
    messageConsumer.close();
    readStream.close();
  }

  @Test
  public void testWriteDb() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    MessageQueue messageQueue = DefaultMessageQueue.create(1000);
    MockConsumerRepository eventConsumerDao = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    MessageConsumer messageConsumer = MessageConsumer
        .create(options, messageQueue, eventConsumerDao);
    messageConsumer.consumer(null, null, e -> {
      int seq = count.incrementAndGet();
      if (seq % 2 == 0) {
        throw new RuntimeException();
      }
    });
    ((MessageConsumerImpl) messageConsumer).start();
    MessageReadStream readStream = new BlockReadStream(messageQueue, eventConsumerDao);
    ((BlockReadStream) readStream).pollAndEnqueue();

    Awaitility.await().until(() -> count.get() == 100);
    long successCount = eventConsumerDao.events().stream()
        .filter(e -> e.header().ext("state") != null)
        .filter(e -> e.header().ext("state").equals(String.valueOf(ConsumeMessageState.SUCCEED.value())))
        .count();
    Assert.assertEquals(successCount, 50);
    long failedCount = eventConsumerDao.events().stream()
        .filter(e -> e.header().ext("state") != null)
        .filter(e -> e.header().ext("state").equals(String.valueOf(ConsumeMessageState.SUCCEED.value())))
        .count();
    Assert.assertEquals(failedCount, 50);
    messageConsumer.close();
    readStream.close();
  }

  @Test
  public void testSeq() {

    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10)
        .setBlockedCheckerMs(300);
    SequentialMessageQueue eventQueue = SequentialMessageQueue.create(e -> {
      Event event = (Event) e.body();
      return event.content().get("deviceId").toString();
    },
        1000);
    MessageConsumerRepository messageConsumerRepository = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    List<Integer> zeroList = new ArrayList<>();
    List<Integer> fiveList = new ArrayList<>();
    MessageConsumer messageConsumer = MessageConsumer.create(options, eventQueue,
        messageConsumerRepository);
    messageConsumer.consumer(null, null, e -> {
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
    messageConsumer.start();
    MessageReadStream readStream = new BlockReadStream(eventQueue, messageConsumerRepository);
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
    messageConsumer.close();
    readStream.close();
  }

  @Test
  public void testPauseAndResume() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    MessageQueue messageQueue = DefaultMessageQueue.create(5);
    MessageConsumerRepository messageConsumerRepository = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    MessageConsumer messageConsumer = MessageConsumer.create(options, messageQueue,
        messageConsumerRepository);
    messageConsumer.consumer(null, null, e -> {
      try {
        TimeUnit.MILLISECONDS.sleep(40);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      count.incrementAndGet();
    });
    messageConsumer.start();
    MessageReadStream readStream = new BlockReadStream(messageQueue, messageConsumerRepository);
    ((BlockReadStream) readStream).pollAndEnqueue();
    Assert.assertTrue(readStream.paused());
    Awaitility.await().until(() -> count.get() == 100);

//    ((BlockReadStream) readStream).pollAndEnqueue();
//    Awaitility.await().until(() -> !readStream.paused());
//    Awaitility.await().until(() -> count.get() == 200);
    messageConsumer.close();
    readStream.close();
  }

  @Test
  public void testScheduler() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    MessageQueue messageQueue = DefaultMessageQueue.create(1000);
    MockConsumerRepository eventConsumerDao = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    MessageConsumer messageConsumer = MessageConsumer
        .create(options, messageQueue, eventConsumerDao);
    messageConsumer.consumer(null, null, e -> {
      int seq = count.incrementAndGet();
      if (seq % 2 == 0) {
        throw new RuntimeException();
      }
    });
    messageConsumer.start();
    MessageReadStream readStream = new BlockReadStream(messageQueue, eventConsumerDao);

    MessageConsumerScheduler scheduler = MessageConsumerScheduler
        .create(eventConsumerDao, messageQueue, 1000L);
    scheduler.start();

    Awaitility.await().until(() -> count.get() == 100);


    Awaitility.await().until(() -> {
      long successCount = eventConsumerDao.events().stream()
          .filter(e -> e.header().ext("state") != null)
          .filter(e -> e.header().ext("state").equals(String.valueOf(ConsumeMessageState.SUCCEED.value())))
          .count();
      return successCount == 50;
    });
    Awaitility.await().until(() -> {
      long failedCount = eventConsumerDao.events().stream()
          .filter(e -> e.header().ext("state") != null)
          .filter(e -> e.header().ext("state").equals(String.valueOf(ConsumeMessageState.SUCCEED.value())))
          .count();
      return failedCount == 50;
    });
    messageConsumer.close();
    readStream.close();
    scheduler.close();
  }
}
