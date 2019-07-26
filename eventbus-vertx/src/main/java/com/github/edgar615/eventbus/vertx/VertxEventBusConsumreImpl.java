package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.bus.ConsumerOptions;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.repository.ConsumeEventState;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VertxEventBusConsumreImpl implements VertxEventBusConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxEventBusConsumer.class);
//  private final BlockedEventChecker checker;

  private final EventQueue eventQueue;

  private VertxEventConsumerRepository consumerRepository;

//  private final long blockedCheckerMs;

  private final Handler<AsyncResult<Event>> resultHandler;

  private final Vertx vertx;

  private long timerId;

  private volatile boolean scheduling = false;

  VertxEventBusConsumreImpl(Vertx vertx, ConsumerOptions options, EventQueue queue,
      VertxEventConsumerRepository consumerRepository) {
    this.vertx = vertx;
    this.eventQueue = queue;
    this.consumerRepository = consumerRepository;
    // TODO blocker
//    this.blockedCheckerMs = options.getBlockedCheckerMs();
//    this.checker = BlockedEventChecker.create(this.blockedCheckerMs);
//    running = true;
    this.resultHandler = ar -> {
      eventQueue.complete(ar.result());
      //处理完成后立刻拉取下一个消息
      schedule(0);
    };
  }

  @Override
  public void start() {
    schedule(100);
  }

  @Override
  public void close() {
    vertx.cancelTimer(timerId);
  }

  @Override
  public void consumer(String topic, String resource, VertxEventHandler handler) {
    VertxHandlerRegistry.instance().register(new VertxHandlerKey(topic, resource), handler);
  }

  @Override
  public long waitForHandle() {
    return 0;
  }

  @Override
  public boolean isRunning() {
    return false;
  }

  private void schedule(long delay) {
    if (delay > 0) {
      timerId = vertx.setTimer(delay, l -> run(resultHandler));
    } else {
      run(resultHandler);
    }
  }

  private void run(Handler<AsyncResult<Event>> resultHandler) {
    Event event = eventQueue.poll();
    if (event == null) {
      //如果没有事件，等待100毫秒
      schedule(100);
      return;
    }
    doHandle(event, resultHandler);
  }

  private void doHandle(Event event, Handler<AsyncResult<Event>> resultHandler) {
    long start = System.currentTimeMillis();
    Future<CompositeFuture> completeFuture = Future.future();
    Collection<VertxEventHandler> handlers = VertxHandlerRegistry.instance()
        .findAllHandler(new VertxHandlerKey(event.head().to(), event.action().resource()));
    if (handlers == null || handlers.isEmpty()) {
      LOGGER.warn(LoggingMarker.getIdLoggingMarker(event.head().id()), "no handler");
      completeFuture.complete();
    } else {
      List<Future> futures = new ArrayList<>();
      for (VertxEventHandler handler : handlers) {
        Future<Void> future = Future.future();
        futures.add(future);
        handler.handle(event, ar -> {
          if (ar.succeeded()) {
            future.complete();
          } else {
            future.fail(ar.cause());
          }
        });
      }
      CompositeFuture.all(futures)
          .setHandler(completeFuture);
    }
    completeFuture.setHandler(ar -> {
      if (ar.succeeded()) {
        long duration = System.currentTimeMillis() - start;
        LOGGER.info(
            LoggingMarker.getLoggingMarker(event.head().id(), ImmutableMap.of("duration", duration)),
            "consume succeed");
        resultHandler.handle(Future.succeededFuture(event));
        if (consumerRepository != null) {
          markSucess(event);
        }
      } else {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "consume failed",
            ar.cause().getMessage());
        resultHandler.handle(Future.failedFuture(ar.cause()));
        if (consumerRepository != null) {
          markFailed(event, ar.cause());
        }
      }
    });
  }

  private void markSucess(Event event) {
    consumerRepository.mark(event.head().id(), ConsumeEventState.SUCCEED, ar -> {
      if (ar.failed()) {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "mark event failed",
            ar.cause());
      }
    });
  }

  private void markFailed(Event event, Throwable throwable) {
    consumerRepository.mark(event.head().id(), ConsumeEventState.FAILED, ar -> {
      if (ar.failed()) {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "mark event failed",
            ar.cause());
      }
    });
  }
}
