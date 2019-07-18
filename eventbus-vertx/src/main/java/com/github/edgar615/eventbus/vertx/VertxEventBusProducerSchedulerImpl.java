package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.bus.EventBusProducerScheduler;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.repository.SendEventState;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 定时从存储中拉取未发送任务进行发送
 *
 * @author Edgar
 */
class VertxEventBusProducerSchedulerImpl implements VertxEventBusProducerScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventBusProducerScheduler.class);

  private static final int DEFAULT_PREIOD = 1000;

  /**
   * 定时从持久层拉取待发送消息的方法
   */
  private long fetchPeriod;

  private final VertxEventProducerRepository eventProducerRepository;

  private final VertxEventBusWriteStream writeStream;

  private final AtomicInteger processing = new AtomicInteger(0);

  private volatile boolean closed = false;

  private long timerId;

  private Vertx vertx;

  VertxEventBusProducerSchedulerImpl(Vertx vertx,
      VertxEventProducerRepository eventProducerRepository,
      VertxEventBusWriteStream writeStream, long fetchPeriod) {
    this.vertx = vertx;
    this.writeStream = writeStream;
    this.eventProducerRepository = eventProducerRepository;
    if (fetchPeriod <= 0) {
      this.fetchPeriod = DEFAULT_PREIOD;
    } else {
      this.fetchPeriod = fetchPeriod;
    }

  }

  @Override
  public void start() {
    LOGGER.info("start producer scheduler, period:{}ms", fetchPeriod);
    schedule(fetchPeriod);
  }

  @Override
  public void close() {
    LOGGER.info("close producer scheduler");
    vertx.cancelTimer(timerId);
  }

  private void schedule(long delay) {
    if (delay > 0) {
      timerId = vertx.setTimer(delay, l -> {
        this.doSchedule();
      });
    } else {
      this.doSchedule();
    }
  }

  private void doSchedule() {
// 如果processing大于0说明有任务在执行，直接返回，在任务执行完成后会重新只执行定时任务
    if (closed || processing.get() > 0) {
      LOGGER.trace("skip scheduler, closed:{}, processing:{}", closed, processing.get());
      return;
    }
    Future<List<Event>> future = Future.future();
    eventProducerRepository.waitingForSend(ar -> {
      if (ar.failed()) {
        future.complete(new ArrayList<>());
        return;
      }
      List<Event> waitingForSend = ar.result();
      LOGGER.trace("{} events to be send", waitingForSend.size());
      //没有数据，等待
      if (waitingForSend.isEmpty()) {
        schedule(fetchPeriod);
        return;
      }
      processing.addAndGet(waitingForSend.size());
      future.complete(waitingForSend);
      future.compose(this::doSend);
    });
  }

  private Future<Void> doSend(List<Event> events) {
    for (Event event : events) {
      Future<Event> future = Future.future();
      writeStream.send(event, future);
      future.setHandler(ar -> {
        int len = processing.addAndGet(-1);
        if (len == 0) {
          // 有数据，立即继续执行
          schedule(0);
        }
        if (ar.succeeded()) {
          markSucess(event);
        } else {
          markFailed(event, ar.cause());
        }
      });
    }
    return Future.future();
  }

  private void markSucess(Event event) {
    LOGGER.info(LoggingMarker.getIdLoggingMarker(event.head().id()), "send succeed");
    eventProducerRepository.mark(event.head().id(), SendEventState.SUCCEED, ar -> {
      if (ar.failed()) {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "mark event failed",
            ar.cause());
      }
    });
  }

  private void markFailed(Event event, Throwable throwable) {
    LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "send failed",
        throwable.getMessage());
    eventProducerRepository.mark(event.head().id(), SendEventState.FAILED, ar -> {
      if (ar.failed()) {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "mark event failed",
            ar.cause());
      }
    });
  }

}
