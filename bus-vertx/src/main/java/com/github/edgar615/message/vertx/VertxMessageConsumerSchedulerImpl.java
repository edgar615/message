package com.github.edgar615.message.vertx;

import com.github.edgar615.message.bus.MessageProducerScheduler;
import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.utils.MessageQueue;
import io.vertx.core.Vertx;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 定时从存储中拉取未发送任务进行发送
 *
 * @author Edgar
 */
class VertxMessageConsumerSchedulerImpl implements VertxMessageConsumerScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducerScheduler.class);

  private static final int DEFAULT_PREIOD = 1000;

  /**
   * 定时从持久层拉取待消费消息的方法
   */
  private long fetchPeriod;

  private final VertxMessageConsumerRepository eventConsumerRepository;

  private final MessageQueue queue;

  private volatile boolean closed = false;

  private long timerId;

  private Vertx vertx;

  VertxMessageConsumerSchedulerImpl(Vertx vertx,
      VertxMessageConsumerRepository eventConsumerRepository,
      MessageQueue queue, long fetchPeriod) {
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
      List<Message> waitingForConsume = ar.result();
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
