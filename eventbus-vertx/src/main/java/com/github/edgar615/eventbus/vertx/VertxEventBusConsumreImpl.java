package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.bus.BlockedEventChecker;
import com.github.edgar615.eventbus.bus.ConsumerOptions;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.repository.EventConsumerRepository;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.NamedThreadFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.HandlerRegistration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class VertxEventBusConsumreImpl implements VertxEventBusConsumer {

//  private final BlockedEventChecker checker;

  private final EventQueue eventQueue;

  private volatile boolean running = false;

  private EventConsumerRepository consumerRepository;

//  private final int workerCount;
//
//  private final long blockedCheckerMs;

  private final Handler<AsyncResult<Event>> resultHandler;

  private final Vertx vertx;

  VertxEventBusConsumreImpl(Vertx vertx, ConsumerOptions options, EventQueue queue,
      EventConsumerRepository consumerRepository) {
    this.vertx = vertx;
    this.eventQueue = queue;
    this.consumerRepository = consumerRepository;
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

  }

  @Override
  public void consumer(String topic, String resource, VertxEventHandler handler) {

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
      vertx.setTimer(delay, l -> run(resultHandler));
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
    //黑名单检查
    long start = System.currentTimeMillis();
    Future<Integer> completeFuture = Future.future();
    List<VertxEventHandler> handlers =
        VertxHandlerRegistry.instance()
            .findAllHandler(event)
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
