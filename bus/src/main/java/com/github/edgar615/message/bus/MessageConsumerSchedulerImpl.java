package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.MessageConsumerRepository;
import com.github.edgar615.message.utils.MessageQueue;
import com.github.edgar615.message.utils.NamedThreadFactory;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 定时从存储中拉取未处理任务处理
 * @author Edgar
 */
class MessageConsumerSchedulerImpl implements MessageConsumerScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumerScheduler.class);

  private static final int DEFAULT_PREIOD = 1000;

  /**
   * 定时从持久层拉取待发送消息的方法
   */
  private long fetchPeriod;

  private final MessageConsumerRepository messageConsumerRepository;

  private final ScheduledExecutorService scheduledExecutor;

  private final MessageQueue queue;

  private volatile boolean closed = false;

  MessageConsumerSchedulerImpl(MessageConsumerRepository messageConsumerRepository,
      MessageQueue queue, long fetchPeriod) {
    this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory.create("consumer-scheduler"));
    this.queue = queue;
    this.messageConsumerRepository = messageConsumerRepository;
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
    scheduledExecutor.shutdown();
  }

  private void schedule(long delay) {
    Runnable scheduledCommand = () -> {
      // 如果processing大于0说明有任务在执行，直接返回，在任务执行完成后会重新只执行定时任务
      if (closed || queue.isFull()) {
        LOGGER.trace("skip scheduler, closed:{}, queue:{}", closed,  queue.size());
        schedule(fetchPeriod);
        return;
      }

      List<Message> waitingForConsume = messageConsumerRepository.waitingForConsume();
      //没有数据，等待
      LOGGER.trace("{} events to be consume", waitingForConsume.size());
      if (waitingForConsume.isEmpty()) {
        schedule(fetchPeriod);
        return;
      }
      queue.enqueue(waitingForConsume);
    };
    if (delay > 0) {
      scheduledExecutor.schedule(scheduledCommand, delay, TimeUnit.MILLISECONDS);
    } else {
      //直接运行
      scheduledExecutor.submit(scheduledCommand);
    }
  }

}
