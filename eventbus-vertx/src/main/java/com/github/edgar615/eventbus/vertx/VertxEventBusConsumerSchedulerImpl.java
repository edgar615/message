package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.bus.EventBusProducerScheduler;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.EventQueue;
import io.vertx.core.Vertx;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 定时从存储中拉取未发送任务进行发送
 *
 * @author Edgar
 */
class VertxEventBusConsumerSchedulerImpl implements VertxEventBusConsumerScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventBusProducerScheduler.class);

  private static final int DEFAULT_PREIOD = 1000;

  /**
   * 定时从持久层拉取待消费消息的方法
   */
  private long fetchPeriod;

  private final VertxEventConsumerRepository eventConsumerRepository;

  private final EventQueue queue;

  private volatile boolean closed = false;

  private long timerId;

  private Vertx vertx;

  VertxEventBusConsumerSchedulerImpl(Vertx vertx,
      VertxEventConsumerRepository eventConsumerRepository,
      EventQueue queue, long fetchPeriod) {
    this.vertx = vertx;
    this.queue = queue;
    this.eventConsumerRepository = eventConsumerRepository;
    if (fetchPeriod <= 0) {
      this.fetchPeriod = DEFAULT_PREIOD;
    } else {
      this.fetchPeriod = fetchPeriod;
    }

  }

  @Override
  public void start() {
    LOGGER.info("start consumer scheduler, period:{}ms", fetchPeriod);
    schedule(fetchPeriod);
  }

  @Override
  public void close() {
    closed = true;
    LOGGER.info("close consumer scheduler");
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
    if (closed || queue.isFull()) {
      LOGGER.trace("skip scheduler, closed:{}, queue:{}", closed, queue.size());
      schedule(fetchPeriod);
      return;
    }
    eventConsumerRepository.waitingForConsume(ar -> {
      if (ar.failed()) {
        schedule(fetchPeriod);
        return;
      }
      List<Event> waitingForConsume = ar.result();
      LOGGER.trace("{} events to be consume", waitingForConsume.size());
      //没有数据，等待
      if (waitingForConsume.isEmpty()) {
        schedule(fetchPeriod);
        return;
      }
      queue.enqueue(waitingForConsume);
      schedule(0);
    });
  }


}
