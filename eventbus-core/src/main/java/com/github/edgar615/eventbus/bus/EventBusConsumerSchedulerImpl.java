package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.dao.EventConsumerDao;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.NamedThreadFactory;
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
public class EventBusConsumerSchedulerImpl implements EventBusConsumerScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventBusConsumerScheduler.class);

  private static final int DEFAULT_PREIOD = 1000;

  /**
   * 定时从持久层拉取待发送消息的方法
   */
  private long fetchPeriod;

  private final EventConsumerDao eventConsumerDao;

  private final ScheduledExecutorService scheduledExecutor;

  private final EventQueue queue;

  private volatile boolean closed = false;

  public EventBusConsumerSchedulerImpl(EventConsumerDao eventConsumerDao,
      EventQueue queue, long fetchPeriod) {
    this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory.create("consumer-scheduler"));
    this.queue = queue;
    this.eventConsumerDao = eventConsumerDao;
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

      List<Event> waitingForConsume = eventConsumerDao.waitingForConsume();
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
