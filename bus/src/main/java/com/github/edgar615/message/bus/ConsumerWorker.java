package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.ConsumeMessageState;
import com.github.edgar615.message.repository.MessageConsumerRepository;
import com.github.edgar615.message.utils.MessageIdTracing;
import com.github.edgar615.message.utils.MessageIdTracingHolder;
import com.github.edgar615.message.utils.MessageQueue;
import com.github.edgar615.message.utils.LoggingMarker;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

class ConsumerWorker implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerWorker.class);
  private final MessageQueue queue;
  private final BlockedMessageChecker checker;
  private final long blockedCheckerMs;
  private final MessageConsumerRepository consumerRepository;

  ConsumerWorker(MessageQueue queue, MessageConsumerRepository consumerRepository,
      BlockedMessageChecker checker, long blockedCheckerMs) {
    this.queue = queue;
    this.consumerRepository = consumerRepository;
    this.checker = checker;
    this.blockedCheckerMs = blockedCheckerMs;
  }

  @Override
  public void run() {
    // todo 改为开关
    while (true) {
      dequeueAndHandle();
    }
  }

  private void dequeueAndHandle() {
    Message message = null;
    BlockedMessageHolder holder = null;
    try {
      message = queue.dequeue();
      MessageIdTracing messageIdTracing = new MessageIdTracing(message.header().id());
      MessageIdTracingHolder.set(messageIdTracing);
      MDC.put("x-request-id", message.header().id());
      long start = System.currentTimeMillis();
      holder = BlockedMessageHolder.create(message.header().id(), blockedCheckerMs);
      if (checker != null) {
        checker.register(holder);
      }
      doHandle(message);
      long duration = System.currentTimeMillis() - start;
      LOGGER.info(
          LoggingMarker.getLoggingMarker(message.header().id(), ImmutableMap.of("duration", duration)),
          "consume succeed");
    } catch (InterruptedException e) {
      LOGGER.warn(LoggingMarker.getIdLoggingMarker(message.header().id()), "thread interrupted");
//        因为中断一个运行在线程池中的任务可以起到双重效果，一是取消任务，二是通知执行线程线程池正要关闭。如果任务生吞中断请求，则 worker
// 线程将不知道有一个被请求的中断，从而耽误应用程序或服务的关闭
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOGGER.error(LoggingMarker.getIdLoggingMarker(message.header().id()), "consume failed", e);
    } finally {
      MessageIdTracingHolder.clear();
      MDC.remove("x-request-id");
      try {
        if (holder != null) {
          holder.completed();
        }
        if (message != null) {
          queue.complete(message);
        }
      } catch (Exception e) {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(message.header().id()), "complete failed", e);
      }
    }
  }

  private void doHandle(Message message) {
    try {
      Collection<MessageHandler> handlers =
          HandlerRegistry.instance()
              .findAllHandler(new HandlerKey(message.header().to(), message.body().resource()));
      if (handlers == null || handlers.isEmpty()) {
        LOGGER.warn(LoggingMarker.getIdLoggingMarker(message.header().id()), "no handler");
      } else {
        for (MessageHandler handler : handlers) {
          handler.handle(message);
        }
      }
      if (consumerRepository != null) {
        markSucess(message);
      }
    } catch (Exception e) {
      if (consumerRepository != null) {
        markFailed(message, e);
      }
      throw e;
    }
  }

  private void markSucess(Message message) {
    try {
      consumerRepository.mark(message.header().id(), ConsumeMessageState.SUCCEED);
    } catch (Exception e) {
      LOGGER.error(LoggingMarker.getIdLoggingMarker(message.header().id()), "mark message failed", e);
    }
  }

  private void markFailed(Message message, Throwable throwable) {
    try {
      consumerRepository.mark(message.header().id(), ConsumeMessageState.FAILED);
    } catch (Exception e) {
      LOGGER.error(LoggingMarker.getIdLoggingMarker(message.header().id()), "mark message failed", e);
    }
  }
}
