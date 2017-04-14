package com.edgar.util.eventbus;

import com.google.common.base.Strings;

import com.edgar.util.concurrent.NamedThreadFactory;
import com.edgar.util.concurrent.OrderQueue;
import com.edgar.util.event.Event;
import com.edgar.util.eventbus.metric.DummyMetrics;
import com.edgar.util.eventbus.metric.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Edgar  Date 2017/3/22
 */
class EventbusImpl implements Eventbus {

  private static final Logger LOGGER = LoggerFactory.getLogger(Eventbus.class);

  private final ScheduledExecutorService scheduledExecutor =
          Executors.newSingleThreadScheduledExecutor(
                  NamedThreadFactory.create("eventbus-schedule"));

  private final ExecutorService producerExecutor
          = Executors.newFixedThreadPool(1, NamedThreadFactory.create("eventbus-producer"));

  private final ExecutorService consumerExecutor
          = Executors.newFixedThreadPool(1, NamedThreadFactory.create("eventbus-consumer"));

  private Metrics metrics = new DummyMetrics();

  private OrderQueue sendQueue;

  private SendStorage sendStorage;

  private SendBackend backend;

  private final Callback callback = (future) -> {
    Event event = future.event();
    long duration = Instant.now().getEpochSecond() - event.head().timestamp();
    metrics.sendEnd(future.succeeded(), duration);
    if (future.succeeded()) {
      LOGGER.info("======> [{}] [OK] [{}] [{}] [{}] [{}]",
                  event.head().id(),
                  event.head().to(),
                  event.head().action(),
                  Helper.toHeadString(event),
                  Helper.toActionString(event));
    } else {
      LOGGER.error("======> [{}] [FAILED] [{}] [{}] [{}] [{}]",
                   event.head().id(),
                   event.head().to(),
                   event.head().action(),
                   Helper.toHeadString(event),
                   Helper.toActionString(event),
                   future.cause().getMessage());
    }
    mark(event, future.succeeded() ? 1 : 2);
  };

  EventbusImpl(ProducerOptions producerOptions, ConsumerOptions consumerOptions) {
    metrics = Metrics.create("eventbus");
    if (producerOptions != null &&
        !Strings.isNullOrEmpty(producerOptions.getServers())) {
      this.backend = new SendBackendImpl(producerOptions);
      sendStorage = producerOptions.getSendStorage();
      sendQueue = new OrderQueue();
      if (sendStorage != null) {
        Runnable scheduledCommand = () -> {
          List<Event> pending = sendStorage.pendingList();
          pending.forEach(e -> send(e));
        };
        long period = producerOptions.getFetchPendingPeriod();
        scheduledExecutor.scheduleAtFixedRate(scheduledCommand, period, period, TimeUnit
                .MILLISECONDS);
      }
    }
    if (consumerOptions != null &&
        !Strings.isNullOrEmpty(consumerOptions.getServers())) {
      ConsumerBackend consumerBackend =
              new ConsumerBackendImpl(consumerOptions, metrics);
      consumerExecutor.submit(consumerBackend);
    }

  }

  @Override
  public void send(Event event) {
    if (sendStorage != null) {
      sendStorage.checkAndSave(event);
    }

    Runnable command = () -> {
      if (event.head().duration() > 0) {
        long current = Instant.now().getEpochSecond();
        if (current > event.head().timestamp() + event.head().duration()) {
          LOGGER.info("---|  [{}] [EXPIRE] [{}] [{}] [{}] [{}]",
                      event.head().id(),
                      event.head().to(),
                      event.head().action(),
                      Helper.toHeadString(event),
                      Helper.toActionString(event));
          mark(event, 3);
        } else {
          backend.send(event)
                  .setCallback(callback);
        }
      }
      metrics.sendStart();
    };
    sendQueue.execute(command, producerExecutor);
    metrics.sendEnqueue();
  }

  @Override
  public Map<String, Object> metrics() {
    return metrics.metrics();
  }

  @Override
  public void consumer(String topic, String resource, EventHandler handler) {
    HandlerRegistration.instance().registerHandler(topic, resource, handler);
  }

  private void mark(Event event, int status) {
    try {
      if (sendStorage != null && sendStorage.shouldStorage(event)) {
        sendStorage.mark(event, status);
      }
    } catch (Exception e) {
      LOGGER.warn("---| [{}] [FAILED] [mark:{}] [{}]",
                  event.head().id(),
                  status,
                  e.getMessage());
    }
  }

}
