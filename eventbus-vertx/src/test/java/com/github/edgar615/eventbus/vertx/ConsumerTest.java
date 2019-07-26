package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.bus.ConsumerOptions;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.repository.ConsumeEventState;
import com.github.edgar615.eventbus.utils.DefaultEventQueue;
import com.github.edgar615.eventbus.utils.EventQueue;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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

  private Vertx vertx;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    VertxHandlerRegistry.instance().unregisterAll(new VertxHandlerKey(null, null));
  }

  @Test
  public void testConsumer() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    EventQueue eventQueue = DefaultEventQueue.create(1000);
    AtomicInteger count = new AtomicInteger();
    VertxEventBusConsumer eventBusConsumer = VertxEventBusConsumer.create(vertx, options, eventQueue);
    VertxEventHandler handler = new VertxEventHandler() {
      @Override
      public void handle(Event event, Handler<AsyncResult<Event>> resultHandler) {
        count.incrementAndGet();
        resultHandler.handle(Future.succeededFuture(event));
      }
    };
    eventBusConsumer.consumer(null, null, handler);
    eventBusConsumer.start();
    VertxEventBusReadStream readStream = new BlockReadStream(eventQueue, null);
    readStream.start();
    ((BlockReadStream) readStream).pollAndEnqueue(ar -> {});

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
    VertxEventBusConsumer eventBusConsumer = VertxEventBusConsumer.create(vertx, options, eventQueue, eventConsumerDao);
    VertxEventHandler handler = new VertxEventHandler() {
      @Override
      public void handle(Event event, Handler<AsyncResult<Event>> resultHandler) {
        int seq = count.incrementAndGet();
        if (seq % 2 == 0) {
          resultHandler.handle(Future.failedFuture(new RuntimeException()));
        } else {
          resultHandler.handle(Future.succeededFuture(event));
        }
      }
    };
    eventBusConsumer.consumer(null, null, handler);
    eventBusConsumer.start();
    VertxEventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerDao);
    readStream.start();
    ((BlockReadStream) readStream).pollAndEnqueue(ar -> {});

    Awaitility.await().until(() -> count.get() == 100);
    long successCount = eventConsumerDao.events().stream()
        .filter(e -> e.head().ext("state") != null)
        .filter(e -> e.head().ext("state").equals(String.valueOf(ConsumeEventState.SUCCEED.value())))
        .count();
    Assert.assertEquals(successCount, 50);
    long failedCount = eventConsumerDao.events().stream()
        .filter(e -> e.head().ext("state") != null)
        .filter(e -> e.head().ext("state").equals(String.valueOf(ConsumeEventState.FAILED.value())))
        .count();
    Assert.assertEquals(failedCount, 50);
    eventBusConsumer.close();
    readStream.close();
  }

  @Test
  public void testPauseAndResume() {
    ConsumerOptions options = new ConsumerOptions().setWorkerPoolSize(10);
    EventQueue eventQueue = DefaultEventQueue.create(5);
    VertxEventConsumerRepository eventConsumerRepository = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    VertxEventBusConsumer eventBusConsumer = VertxEventBusConsumer.create(vertx, options, eventQueue,
        eventConsumerRepository);
    VertxEventHandler handler = new VertxEventHandler() {
      @Override
      public void handle(Event event, Handler<AsyncResult<Event>> resultHandler) {
        try {
          TimeUnit.MILLISECONDS.sleep(40);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        count.incrementAndGet();
        resultHandler.handle(Future.failedFuture(new RuntimeException()));
      }
    };

    eventBusConsumer.consumer(null, null, handler);
    eventBusConsumer.start();
    VertxEventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerRepository);
    readStream.start();
    ((BlockReadStream) readStream).pollAndEnqueue(ar -> {});

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
    EventQueue eventQueue = DefaultEventQueue.create(5);
    MockConsumerRepository eventConsumerRepository = new MockConsumerRepository();
    AtomicInteger count = new AtomicInteger();
    VertxEventBusConsumer eventBusConsumer = VertxEventBusConsumer.create(vertx, options, eventQueue,
        eventConsumerRepository);
    VertxEventHandler handler = new VertxEventHandler() {
      @Override
      public void handle(Event event, Handler<AsyncResult<Event>> resultHandler) {
        try {
          TimeUnit.MILLISECONDS.sleep(40);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        int seq = count.incrementAndGet();
        if (seq % 2 == 0) {
          resultHandler.handle(Future.failedFuture(new RuntimeException()));
        } else {
          resultHandler.handle(Future.succeededFuture(event));
        }
      }
    };

    eventBusConsumer.consumer(null, null, handler);
    eventBusConsumer.start();
    VertxEventBusReadStream readStream = new BlockReadStream(eventQueue, eventConsumerRepository);
    readStream.start();

    VertxEventBusConsumerScheduler scheduler = VertxEventBusConsumerScheduler.create(vertx, eventConsumerRepository, eventQueue, 1000L);
    scheduler.start();

    Awaitility.await().until(() -> count.get() == 100);
    eventBusConsumer.close();
    readStream.close();
    scheduler.close();

    Awaitility.await().until(() -> {
      long successCount = eventConsumerRepository.events().stream()
          .filter(e -> e.head().ext("state") != null)
          .filter(e -> e.head().ext("state").equals(String.valueOf(ConsumeEventState.SUCCEED.value())))
          .count();
      return successCount == 50;
    });
    Awaitility.await().until(() -> {
      long failedCount = eventConsumerRepository.events().stream()
          .filter(e -> e.head().ext("state") != null)
          .filter(e -> e.head().ext("state").equals(String.valueOf(ConsumeEventState.SUCCEED.value())))
          .count();
      return failedCount == 50;
    });

  }

}
