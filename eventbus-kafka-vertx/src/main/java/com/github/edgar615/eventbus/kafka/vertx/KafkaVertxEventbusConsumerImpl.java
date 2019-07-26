package com.github.edgar615.eventbus.kafka.vertx;

import com.github.edgar615.eventbus.bus.HandlerRegistration;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.kafka.KafkaReadOptions;
import com.github.edgar615.eventbus.metrics.ConsumerMetrics;
import com.github.edgar615.eventbus.metrics.DummyMetrics;
import com.github.edgar615.eventbus.utils.DefaultEventQueue;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.SequentialEventQueue;
import com.github.edgar615.eventbus.vertx.VertxEventConsumerRepository;
import com.github.edgar615.util.eventbus.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
class KafkaVertxEventbusConsumerImpl implements KafkaVertxEventbusConsumer {

  private static final Logger LOGGER =
          LoggerFactory.getLogger(KafkaVertxEventbusConsumerImpl.class);

  private static final String LOG_TYPE = "kafka-consumer";

  private final Vertx vertx;

  private final KafkaReadStream readStream;

  private final EventQueue eventQueue;

  private final Handler<AsyncResult<Event>> resultHandler;

  private final VertxEventConsumerRepository consumerStorage;

  private Function<Event, Boolean> blackListFilter = event -> false;

  private final ConsumerMetrics metrics;

  KafkaVertxEventbusConsumerImpl(Vertx vertx, KafkaReadOptions options) {
    this(vertx, options, null, null, null);
  }

  KafkaVertxEventbusConsumerImpl(Vertx vertx, KafkaReadOptions options,
                                        VertxEventConsumerRepository consumerStorage,
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
      LOGGER.warn("[{}] [EC] [blacklist]", event.head().id());
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
        LOGGER.warn("[{}] [EC] [no handler]", event.head().id());
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

  @Override
  public boolean isRunning() {
    return readStream.isRunning();
  }

  @Override
  public boolean paused() {
    return readStream.paused();
  }
}
