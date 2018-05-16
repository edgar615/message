package com.github.edgar615.util.eventbus.vertx;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.eventbus.DefaultEventQueue;
import com.github.edgar615.util.eventbus.EventQueue;
import com.github.edgar615.util.eventbus.HandlerRegistration;
import com.github.edgar615.util.eventbus.KafkaConsumerOptions;
import com.github.edgar615.util.eventbus.KafkaReadStream;
import com.github.edgar615.util.eventbus.SequentialEventQueue;
import com.github.edgar615.util.log.Log;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
public class VertxEventbusKafkaConsumerImpl implements VertxEventbusKafkaConsumer {

  private static final Logger LOGGER =
          LoggerFactory.getLogger(VertxEventbusKafkaConsumerImpl.class);

  private static final String LOG_TYPE = "eventbus-consumer";

  private final Vertx vertx;

  private final KafkaReadStream readStream;

  private final EventQueue eventQueue;

  private final Handler<AsyncResult<Event>> resultHandler;

  private final VertxConsumerStorage consumerStorage;

  private Function<Event, Boolean> blackListFilter = event -> false;

  public VertxEventbusKafkaConsumerImpl(Vertx vertx, KafkaConsumerOptions options) {
    this(vertx, options, null, null, null);
  }

  public VertxEventbusKafkaConsumerImpl(Vertx vertx, KafkaConsumerOptions options,
                                        VertxConsumerStorage consumerStorage,
                                        Function<Event, String> identificationExtractor,
                                        Function<Event, Boolean> blackListFilter) {
    this.vertx = vertx;
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
              .setLogType("event-consumer")
              .setEvent("blacklist")
              .setTraceId(event.head().id())
              .info();
//      if (consumerStorage != null && consumerStorage.shouldStorage(event)) {
//        consumerStorage.mark(event, 3);
//      }
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

  private void persistResult(Event event, AsyncResult<Event> asyncResult,
                             Handler<AsyncResult<Event>> resultHandler) {
    if (asyncResult.failed()) {
      consumerStorage.mark(event, 2, ar -> {
        resultHandler.handle(Future.succeededFuture(event));
      });
      return;
    }
    consumerStorage.mark(event, 1, ar -> {
      resultHandler.handle(Future.succeededFuture(event));
    });
  }

  private void doHandle(Event event, Handler<AsyncResult<Event>> resultHandler) {
    //黑名单检查
    if (isBlackList(event)) {
      resultHandler.handle(Future.succeededFuture(event));
      return;
    }
    List<VertxEventHandler> handlers =
            HandlerRegistration.instance()
                    .getHandlers(event)
                    .stream().filter(h -> h instanceof VertxEventHandler)
                    .map(h -> (VertxEventHandler) h)
                    .collect(Collectors.toList());
    Future<Void> completeFuture = Future.future();
    if (handlers == null || handlers.isEmpty()) {
      Log.create(LOGGER)
              .setLogType("eventbus-consumer")
              .setEvent("handle")
              .setTraceId(event.head().id())
              .setMessage("NO HANDLER")
              .warn();
      completeFuture.complete();
    } else {
      List<Future> futures = new ArrayList<>();
      for (VertxEventHandler handler : handlers) {
        Future<Void> future = Future.future();
        futures.add(future);
        handler.handle(event, future);
      }
      CompositeFuture.all(futures)
              .setHandler(ar -> {
                completeFuture.complete();
              });
    }
    //持久化
    if (consumerStorage != null && consumerStorage.shouldStorage(event)) {
      completeFuture.setHandler(ar -> {
        consumerStorage.mark(event, ar.succeeded() ? 1 : 2,
                             par -> resultHandler.handle(Future.succeededFuture(event)));
      });
    } else {
      completeFuture.setHandler(ar -> resultHandler.handle(Future.succeededFuture(event)));
    }
  }
}