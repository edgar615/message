package com.github.edgar615.message.vertx;

import com.github.edgar615.message.bus.BlockedMessageChecker;
import com.github.edgar615.message.bus.BlockedMessageHolder;
import com.github.edgar615.message.bus.ConsumerOptions;
import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.ConsumeMessageState;
import com.github.edgar615.message.utils.MessageQueue;
import com.github.edgar615.message.utils.LoggingMarker;
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

class VertxMessageConsumreImpl implements VertxMessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxMessageConsumer.class);
  private final BlockedMessageChecker checker;

  private final MessageQueue messageQueue;

  private VertxMessageConsumerRepository consumerRepository;

  private final long blockedCheckerMs;

  private final Handler<AsyncResult<Message>> resultHandler;

  private final Vertx vertx;

  private long timerId;

  VertxMessageConsumreImpl(Vertx vertx, ConsumerOptions options, MessageQueue queue,
      VertxMessageConsumerRepository consumerRepository) {
    this.vertx = vertx;
    this.messageQueue = queue;
    this.consumerRepository = consumerRepository;
    // TODO blocker
    this.blockedCheckerMs = options.getBlockedCheckerMs();
    this.checker = BlockedMessageChecker.create(this.blockedCheckerMs);
//    running = true;
    this.resultHandler = ar -> {
      messageQueue.complete(ar.result());
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
    checker.close();
  }

  @Override
  public void consumer(String topic, String resource, VertxMessageHandler handler) {
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

  private void run(Handler<AsyncResult<Message>> resultHandler) {
    Message message = messageQueue.poll();
    if (message == null) {
      //如果没有事件，等待100毫秒
      schedule(100);
      return;
    }
    doHandle(message, resultHandler);
  }

  private void doHandle(Message message, Handler<AsyncResult<Message>> resultHandler) {
    BlockedMessageHolder holder = BlockedMessageHolder
        .create(message.header().id(), blockedCheckerMs);
    if (checker != null) {
      checker.register(holder);
    }
    long start = System.currentTimeMillis();
    Future<CompositeFuture> completeFuture = Future.future();
    Collection<VertxMessageHandler> handlers = VertxHandlerRegistry.instance()
        .findAllHandler(new VertxHandlerKey(message.header().to(), message.body().resource()));
    if (handlers == null || handlers.isEmpty()) {
      LOGGER.warn(LoggingMarker.getIdLoggingMarker(message.header().id()), "no handler");
      completeFuture.complete();
    } else {
      List<Future> futures = new ArrayList<>();
      for (VertxMessageHandler handler : handlers) {
        Future<Void> future = Future.future();
        futures.add(future);
        handler.handle(message, ar -> {
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
            LoggingMarker.getLoggingMarker(message.header().id(), ImmutableMap.of("duration", duration)),
            "consume succeed");
        resultHandler.handle(Future.succeededFuture(message));
        if (consumerRepository != null) {
          markSucess(message);
        }
      } else {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(message.header().id()), "consume failed",
            ar.cause().getMessage());
        resultHandler.handle(Future.failedFuture(ar.cause()));
        if (consumerRepository != null) {
          markFailed(message, ar.cause());
        }
      }
    });
  }

  private void markSucess(Message message) {
    consumerRepository.mark(message.header().id(), ConsumeMessageState.SUCCEED, ar -> {
      if (ar.failed()) {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(message.header().id()), "mark message failed",
            ar.cause());
      }
    });
  }

  private void markFailed(Message message, Throwable throwable) {
    consumerRepository.mark(message.header().id(), ConsumeMessageState.FAILED, ar -> {
      if (ar.failed()) {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(message.header().id()), "mark message failed",
            ar.cause());
      }
    });
  }
}
