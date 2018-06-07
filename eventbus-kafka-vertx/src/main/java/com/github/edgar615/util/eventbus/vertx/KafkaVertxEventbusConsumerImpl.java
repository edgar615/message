package com.github.edgar615.util.eventbus.vertx;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.eventbus.DefaultEventQueue;
import com.github.edgar615.util.eventbus.EventQueue;
import com.github.edgar615.util.eventbus.HandlerRegistration;
import com.github.edgar615.util.eventbus.KafkaConsumerOptions;
import com.github.edgar615.util.eventbus.KafkaReadStream;
import com.github.edgar615.util.eventbus.SequentialEventQueue;
import com.github.edgar615.util.log.Log;
import com.github.edgar615.util.metrics.ConsumerMetrics;
import com.github.edgar615.util.metrics.DummyMetrics;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
class KafkaVertxEventbusConsumerImpl implements KafkaVertxEventbusConsumer {

  private static final Logger LOGGER =
          LoggerFactory.getLogger(KafkaVertxEventbusConsumerImpl.class);

  private static final String LOG_TYPE = "eventbus-consumer";

  private final Vertx vertx;

  private final KafkaReadStream readStream;

  private final EventQueue eventQueue;

  private final Handler<AsyncResult<Event>> resultHandler;

  private final VertxConsumerStorage consumerStorage;

  private Function<Event, Boolean> blackListFilter = event -> false;

  private final ConsumerMetrics metrics;

  KafkaVertxEventbusConsumerImpl(Vertx vertx, KafkaConsumerOptions options) {
    this(vertx, options, null, null, null);
  }

  KafkaVertxEventbusConsumerImpl(Vertx vertx, KafkaConsumerOptions options,
                                        VertxConsumerStorage consumerStorage,
                                        Function<Event, String> identificationExtractor,
                                        Function<Event, Boolean> blackListFilter) {
    this.vertx = vertx;
    this.metrics = createMetrics();
    this.consumerStorage = consumerStorage;
    if (blackListFilter == null) {
      this.blackListFilter = e -> false;
    } else {
      this.blackListFilter = blackListFilter;
    }
    if (identificationExtractor == null) {
      this.eventQueue = new DefaultEventQueue(options.getMaxQuota());
    } else {
      this.eventQueue = new SequentialEventQueue(identificationExtractor, options.getMaxQuota());
    }
    this.resultHandler = ar -> {
      eventQueue.complete(ar.result());
      //处理完成后立刻拉取下一个消息
      schedule(0);
    };
    schedule(100);
    this.readStream = new KafkaReadStream(options) {

      @Override
      public void handleEvents(List<Event> events) {
        eventQueue.enqueue(events);
      }

      @Override
      public boolean checkPauseCondition() {
        return eventQueue.isFull();
      }

      @Override
      public boolean checkResumeCondition() {
        return eventQueue.isLowWaterMark();
      }
    };

  }

  @Override
  public void pause() {
    readStream.pause();
  }

  @Override
  public void resume() {
    readStream.resume();
  }

  @Override
  public void close() {
    readStream.close();
  }

  @Override
  public long waitForHandle() {
    return eventQueue.size();
  }

  @Override
  public Map<String, Object> metrics() {
    return metrics.metrics();
  }

  private ConsumerMetrics createMetrics() {
    ServiceLoader<ConsumerMetrics> metrics = ServiceLoader.load(ConsumerMetrics.class);
    Iterator<ConsumerMetrics> iterator = metrics.iterator();
    if (!iterator.hasNext()) {
      return new DummyMetrics();
    } else {
      return iterator.next();
    }
  }


  private void schedule(long delay) {
    if (delay > 0) {
      vertx.setTimer(delay, l -> run(resultHandler));
    } else {
      run(resultHandler);
    }
  }

  private boolean isBlackList(Event event) {
    if (blackListFilter != null && blackListFilter.apply(event)) {
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("blacklist")
              .setTraceId(event.head().id())
              .info();
      return true;
    }
    return false;
  }

  private void run(Handler<AsyncResult<Event>> resultHandler) {
    Event event = eventQueue.poll();
    if (event == null) {
      //如果没有事件，等待100毫秒
      schedule(100);
      return;
    }
    if (consumerStorage != null && consumerStorage.shouldStorage(event)) {
      consumerStorage.isConsumed(event, ar -> {
        if (ar.failed()) {
          resultHandler.handle(Future.succeededFuture(event));
          return;
        }
        if (ar.result()) {
          resultHandler.handle(Future.succeededFuture(event));
        } else {
          doHandle(event, resultHandler);
        }
      });
      return;
    }
    doHandle(event, resultHandler);
  }

  private void doHandle(Event event, Handler<AsyncResult<Event>> resultHandler) {
    metrics.consumerStart();
    //黑名单检查
    long start = System.currentTimeMillis();
    Future<Integer> completeFuture = Future.future();
    if (isBlackList(event)) {
      completeFuture.complete(3);
    } else {
      List<VertxEventHandler> handlers =
              HandlerRegistration.instance()
                      .getHandlers(event)
                      .stream().filter(h -> h instanceof VertxEventHandler)
                      .map(h -> (VertxEventHandler) h)
                      .collect(Collectors.toList());
      if (handlers == null || handlers.isEmpty()) {
        Log.create(LOGGER)
                .setLogType(LOG_TYPE)
                .setEvent("handle")
                .setTraceId(event.head().id())
                .setMessage("NO HANDLER")
                .warn();
        completeFuture.complete(1);
      } else {
        List<Future> futures = new ArrayList<>();
        for (VertxEventHandler handler : handlers) {
          Future<Void> future = Future.future();
          futures.add(future);
          handler.handle(event, future);
        }
        CompositeFuture.all(futures)
                .setHandler(ar -> completeFuture.complete(ar.succeeded() ? 1 : 2));
      }
    }
    //持久化
    if (consumerStorage != null && consumerStorage.shouldStorage(event)) {
      completeFuture.setHandler(ar -> {
        consumerStorage.mark(event, ar.succeeded() ? ar.result() : 2,
                             par -> {
                               metrics.consumerEnd(System.currentTimeMillis() - start);
                               resultHandler.handle(Future.succeededFuture(event));
                             });
      });
    } else {
      completeFuture.setHandler(ar ->{
        metrics.consumerEnd(System.currentTimeMillis() - start);
        resultHandler.handle(Future.succeededFuture(event));
      });
    }
  }
}
