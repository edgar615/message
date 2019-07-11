package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.dao.EventProducerDao;
import com.github.edgar615.eventbus.dao.SendEventState;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import com.github.edgar615.eventbus.utils.NamedThreadFactory;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Edgar
 */
public class EventBusSchedulerImpl implements EventBusScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventBusScheduler.class);

  private static final int DEFAULT_PREIOD = 1000;

  /**
   * 定时从持久层拉取待发送消息的方法
   */
  private long fetchPendingPeriod;

  private final EventProducerDao eventProducerDao;

  private final ScheduledExecutorService scheduledExecutor;

  private final EventBusWriteStream writeStream;

  private final AtomicInteger processing = new AtomicInteger(0);

  private volatile boolean closed = false;

  public EventBusSchedulerImpl(EventProducerDao eventProducerDao,
      EventBusWriteStream writeStream, long fetchPendingPeriod) {
    this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory.create("event-scheduler"));
    this.writeStream = writeStream;
    this.eventProducerDao = eventProducerDao;
    if (fetchPendingPeriod <= 0) {
      this.fetchPendingPeriod = DEFAULT_PREIOD;
    } else {
      this.fetchPendingPeriod = fetchPendingPeriod;
    }

  }

  @Override
  public void start() {
    LOGGER.info("start scheduler, period:{}ms", fetchPendingPeriod);
    schedule(fetchPendingPeriod);
  }

  @Override
  public void close() {
    LOGGER.info("close scheduler");
    scheduledExecutor.shutdown();
  }

  @Override
  public ScheduledExecutorService executor() {
    return scheduledExecutor;
  }

  private void schedule(long delay) {
    Runnable scheduledCommand = () -> {
      // 如果processing大于0说明有任务在执行，直接返回，在任务执行完成后会重新只执行定时任务
      if (closed || processing.get() > 0) {
        LOGGER.trace("skip scheduler, closed:{}, processing:{}", closed,  fetchPendingPeriod);
        return;
      }
      List<Event> waitingForSend = eventProducerDao.waitingForSend();
      //没有数据，等待
      LOGGER.trace("{} events to be send", waitingForSend.size());
      if (waitingForSend.isEmpty()) {
        schedule(fetchPendingPeriod);
        return;
      }
      processing.addAndGet(waitingForSend.size());
      for (Event event : waitingForSend) {
        CompletableFuture<Event> future = writeStream.send(event);
        // 这里是不是应该用一个新的线程处理回调？
        future.thenAccept(this::markSucess)
            .exceptionally(throwable -> {
              markFailed(event, throwable);
              return null;
            }).thenAccept(v -> {
          int len = processing.addAndGet(-1);
          if (len == 0) {
            // 有数据，立即继续执行
            schedule(0);
          }
        });
      }
    };
    if (delay > 0) {
      scheduledExecutor.schedule(scheduledCommand, delay, TimeUnit.MILLISECONDS);
    } else {
      //直接运行
      scheduledExecutor.submit(scheduledCommand);
    }
  }

  private void markSucess(Event event) {
    LOGGER.info(LoggingMarker.getIdLoggingMarker(event.head().id()), "send succeed");
    try {
      eventProducerDao.mark(event.head().id(), SendEventState.SUCCEED);
    } catch (Exception e) {
      LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()),"mark event failed", e);
    }
  }

  private void markFailed(Event event, Throwable throwable) {
    LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()), "send failed", throwable.getMessage());
    try {
      eventProducerDao.mark(event.head().id(), SendEventState.FAILED);
    } catch (Exception e) {
      LOGGER.error(LoggingMarker.getIdLoggingMarker(event.head().id()),"mark event failed", e);
    }
  }

}
