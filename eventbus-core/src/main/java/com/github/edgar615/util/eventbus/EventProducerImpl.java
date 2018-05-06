package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.concurrent.NamedThreadFactory;
import com.github.edgar615.util.concurrent.OrderQueue;
import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.exception.DefaultErrorCode;
import com.github.edgar615.util.exception.SystemException;
import com.github.edgar615.util.log.Log;
import com.github.edgar615.util.metrics.DummyMetrics;
import com.github.edgar615.util.metrics.Metrics;
import com.github.edgar615.util.metrics.ProducerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/4/19.
 *
 * @author Edgar  Date 2017/4/19
 */
public abstract class EventProducerImpl implements EventProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventProducer.class);

  private final ScheduledExecutorService scheduledExecutor =
          Executors.newSingleThreadScheduledExecutor(
                  NamedThreadFactory.create("eventbus-scheduler"));

  private final ExecutorService producerExecutor
          = Executors.newFixedThreadPool(1, NamedThreadFactory.create("eventbus-producer"));

  private final OrderQueue queue = new OrderQueue();

  private final ProducerMetrics metrics;

  private final Callback callback;

  private final long maxQuota;

  private final long fetchPendingPeriod;

  private ProducerStorage producerStorage;

  protected EventProducerImpl(ProducerOptions options) {
    this.maxQuota = options.getMaxQuota();
    this.fetchPendingPeriod = options.getFetchPendingPeriod();
    this.metrics = createMetrics();
    this.callback = (future) -> {
      Event event = future.event();
      long duration = Instant.now().getEpochSecond() - event.head().timestamp();
      metrics.sendEnd(future.succeeded(), duration);
      mark(event, future.succeeded() ? 1 : 2);
    };
    Runtime.getRuntime().addShutdownHook(createHookTask());
  }

  public abstract EventFuture<Void> sendEvent(Event event);

  public EventProducerImpl setProducerStorage(ProducerStorage producerStorage) {
    this.producerStorage = producerStorage;
    if (producerStorage != null) {
      Runnable scheduledCommand = () -> {
        List<Event> pending = producerStorage.pendingList();
        pending.forEach(e -> send(e));
      };
      scheduledExecutor
              .scheduleAtFixedRate(scheduledCommand, fetchPendingPeriod, fetchPendingPeriod,
                      TimeUnit
                              .MILLISECONDS);
    }
    return this;
  }

  @Override
  public void send(Event event) {
    boolean persisted = false;
    if (producerStorage != null) {
      persisted = producerStorage.checkAndSave(event);
    }

    if (queue.size() > maxQuota) {
      Log.create(LOGGER)
              .setLogType("eventbus-producer")
              .setEvent("THROTTLE")
              .setTraceId(event.head().id())
              .setMessage("[{}] [{}] [{}] [{}]")
              .addArg(event.head().to())
              .addArg(event.head().action())
              .addArg(Helper.toHeadString(event))
              .addArg(Helper.toActionString(event))
              .info();
      //持久化的消息，就认为成功，可以直接返回，未持久化的消息拒绝
      if (persisted) {
        return;
      } else {
        throw SystemException.create(DefaultErrorCode.TOO_MANY_REQ)
                .set("maxQuota", maxQuota);
      }
    }

    Runnable command = () -> {
      long current = Instant.now().getEpochSecond();
      if (event.head().duration() > 0
              && current > event.head().timestamp() + event.head().duration()) {
        Log.create(LOGGER)
                .setLogType("eventbus-producer")
                .setEvent("EXPIRE")
                .setTraceId(event.head().id())
                .setMessage("[{}] [{}] [{}] [{}]")
                .addArg(event.head().to())
                .addArg(event.head().action())
                .addArg(Helper.toHeadString(event))
                .addArg(Helper.toActionString(event))
                .info();
        mark(event, 3);
      } else {
        sendEvent(event)
                .setCallback(callback);
      }
      metrics.sendStart();
    };
    queue.execute(command, producerExecutor);
    metrics.sendEnqueue();
  }

  @Override
  public void close() {
    producerExecutor.shutdown();
    scheduledExecutor.shutdown();
  }

  @Override
  public Map<String, Object> metrics() {
    return metrics.metrics();
  }

  private ProducerMetrics createMetrics() {
    ServiceLoader<ProducerMetrics> metrics = ServiceLoader.load(ProducerMetrics.class);
    Iterator<ProducerMetrics> iterator = metrics.iterator();
    if (!iterator.hasNext()) {
      return new DummyMetrics();
    } else {
      return iterator.next();
    }
  }

  private Thread createHookTask() {
    return new Thread() {
      @Override
      public void run() {
        close();
        //等待任务处理完成
        ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) producerExecutor;
        long start = System.currentTimeMillis();
        while (poolExecutor.getTaskCount() - poolExecutor.getCompletedTaskCount() > 0) {
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          Log.create(LOGGER)
                  .setLogType("eventbus-producer")
                  .setEvent("close.waiting")
                  .addData("remaing", poolExecutor.getTaskCount() - poolExecutor.getCompletedTaskCount())
                  .addData("duration", System.currentTimeMillis() - start)
                  .info();
        }
        Log.create(LOGGER)
                .setLogType("eventbus-producer")
                .setEvent("closed")
                .info();
      }
    };
  }

  private void mark(Event event, int status) {
    try {
      if (producerStorage != null && producerStorage.shouldStorage(event)) {
        producerStorage.mark(event, status);
        Log.create(LOGGER)
                .setLogType("eventbus-producer")
                .setEvent("mark")
                .addData("status", status)
                .setTraceId(event.head().id())
                .debug();
      }
    } catch (Exception e) {
      Log.create(LOGGER)
              .setLogType("eventbus-producer")
              .setEvent("mark.failed")
              .addData("status", status)
              .setTraceId(event.head().id())
              .setMessage(e.getMessage())
              .warn();
    }
  }
}
