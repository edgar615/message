package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.dao.ConsumeEventState;
import com.github.edgar615.eventbus.dao.EventConsumerDao;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.EventIdTracing;
import com.github.edgar615.eventbus.utils.EventIdTracingHolder;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

class ConsumerWorker implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerWorker.class);
  private final EventQueue queue;
  private final BlockedEventChecker checker;
  private final long blockedCheckerMs;
  private final EventConsumerDao consumerDao;

  ConsumerWorker(EventQueue queue, EventConsumerDao consumerDao,
      BlockedEventChecker checker, long blockedCheckerMs) {
    this.queue = queue;
    this.consumerDao = consumerDao;
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
    Event event = null;
    BlockedEventHolder holder = null;
    try {
      event = queue.dequeue();
      EventIdTracing eventIdTracing = new EventIdTracing(event.head().id());
      EventIdTracingHolder.set(eventIdTracing);
      MDC.put("x-request-id", event.head().id());
      long start = System.currentTimeMillis();
      holder = BlockedEventHolder.create(event.head().id(), blockedCheckerMs);
      if (checker != null) {
        checker.register(holder);
      }
      doHandle(event);
      long duration = System.currentTimeMillis() - start;
      LOGGER.info(
          LoggingMarker.getLoggingMarker(event.head().id(), ImmutableMap.of("duration", duration)),
          "consume succeed");
    } catch (InterruptedException e) {
      LOGGER.warn(LoggingMarker.getIdLoggingMarker(event.head().id()), "thread interrupted");
//        因为中断一个运行在线程池中的任务可以起到双重效果，一是取消任务，二是通知执行线程线程池正要关闭。如果任务生吞中断请求，则 worker
// 线程将不知道有一个被请求的中断，从而耽误应用程序或服务的关闭
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "consume failed", e);
    } finally {
      EventIdTracingHolder.clear();
      MDC.remove("x-request-id");
      try {
        if (holder != null) {
          holder.completed();
        }
        if (event != null) {
          queue.complete(event);
        }
      } catch (Exception e) {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "complete failed", e);
      }
    }
  }

  private void doHandle(Event event) {
    try {
      Collection<EventConsumer> subscribers =
          ConsumerRegistry.instance()
              .findAllSubscribers(new ConsumerKey(event.head().to(), event.action().resource()));
      if (subscribers == null || subscribers.isEmpty()) {
        LOGGER.warn(LoggingMarker.getIdLoggingMarker(event.head().id()), "no subscriber");
      } else {
        for (EventConsumer subscriber : subscribers) {
          subscriber.subscribe(event);
        }
      }
      if (consumerDao != null) {
        markSucess(event);
      }
    } catch (Exception e) {
      if (consumerDao != null) {
        markFailed(event, e);
      }
      throw e;
    }
  }

  private void markSucess(Event event) {
    try {
      consumerDao.mark(event.head().id(), ConsumeEventState.SUCCEED);
    } catch (Exception e) {
      LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "mark event failed", e);
    }
  }

  private void markFailed(Event event, Throwable throwable) {
    LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "consume failed",
        throwable.getMessage());
    try {
      consumerDao.mark(event.head().id(), ConsumeEventState.FAILED);
    } catch (Exception e) {
      LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "mark event failed", e);
    }
  }
}
