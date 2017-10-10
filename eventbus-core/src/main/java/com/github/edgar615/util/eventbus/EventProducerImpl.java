package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.concurrent.NamedThreadFactory;
import com.github.edgar615.util.concurrent.OrderQueue;
import com.github.edgar615.util.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/4/19.
 *
 * @author Edgar  Date 2017/4/19
 */
public abstract class EventProducerImpl implements EventProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventProducer.class);

  private final ScheduledExecutorService scheduledExecutor =
          Executors.newSingleThreadScheduledExecutor(
                  NamedThreadFactory.create("eventbus-schedule"));

  private final ExecutorService producerExecutor
          = Executors.newFixedThreadPool(1, NamedThreadFactory.create("eventbus-producer"));


  private final OrderQueue queue = new OrderQueue();

  private final ProducerStorage producerStorage;

  private final Metrics metrics;

  private final Callback callback;

  private final long maxQuota;

  protected EventProducerImpl(ProducerOptions options) {
    this.maxQuota = options.getMaxQuota();
    this.producerStorage = options.getProducerStorage();
    this.metrics = options.getMetrics();
    if (producerStorage != null) {
      Runnable scheduledCommand = () -> {
        List<Event> pending = producerStorage.pendingList();
        pending.forEach(e -> send(e));
      };
      long period = options.getFetchPendingPeriod();
      scheduledExecutor.scheduleAtFixedRate(scheduledCommand, period, period, TimeUnit
              .MILLISECONDS);
    }
    this.callback = (future) -> {
      Event event = future.event();
      long duration = Instant.now().getEpochSecond() - event.head().timestamp();
      metrics.sendEnd(future.succeeded(), duration);
      mark(event, future.succeeded() ? 1 : 2);
    };
  }

  public abstract EventFuture<Void> sendEvent(Event event);

  @Override
  public void send(Event event) {
    if (producerStorage != null) {
      producerStorage.checkAndSave(event);
    }

    if (queue.size() > maxQuota) {
      LOGGER.info("---|  [{}] [THROTTLE] [{}] [{}] [{}] [{}]",
                  event.head().id(),
                  event.head().to(),
                  event.head().action(),
                  Helper.toHeadString(event),
                  Helper.toActionString(event));
      return;
    }

    Runnable command = () -> {
      long current = Instant.now().getEpochSecond();
      if (event.head().duration() > 0
          && current > event.head().timestamp() + event.head().duration()) {
        LOGGER.info("---|  [{}] [EXPIRE] [{}] [{}] [{}] [{}]",
                    event.head().id(),
                    event.head().to(),
                    event.head().action(),
                    Helper.toHeadString(event),
                    Helper.toActionString(event));
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

  private void mark(Event event, int status) {
    try {
      if (producerStorage != null && producerStorage.shouldStorage(event)) {
        producerStorage.mark(event, status);
      }
    } catch (Exception e) {
      LOGGER.warn("---| [{}] [FAILED] [mark:{}] [{}]",
                  event.head().id(),
                  status,
                  e.getMessage());
    }
  }
}
