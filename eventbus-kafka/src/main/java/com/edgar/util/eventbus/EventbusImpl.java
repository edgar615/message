package com.edgar.util.eventbus;

import com.google.common.base.Strings;

import com.edgar.util.concurrent.NamedThreadFactory;
import com.edgar.util.event.Event;
import com.edgar.util.eventbus.metric.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(EventbusImpl.class);

  private final List<HandlerBinding> bindings = new ArrayList<>();

  private final ScheduledExecutorService scheduledExecutor =
          Executors.newSingleThreadScheduledExecutor(
                  NamedThreadFactory.create("eventbus-schedule"));

  private final ExecutorService workerExecutor =
          Executors.newCachedThreadPool(NamedThreadFactory.create("eventbus-worker"));

  private final ExecutorService producerExecutor
          = Executors.newFixedThreadPool(1, NamedThreadFactory.create("eventbus-producer"));

  private final ExecutorService consumerExecutor
          = Executors.newFixedThreadPool(1, NamedThreadFactory.create("eventbus-consumer"));

  private final Metrics metrics;

  private SendQueue sendQueue;

  private SendStorage sendStorage;

  EventbusImpl(ProducerOptions producerOptions, ConsumerOptions consumerOptions) {
    metrics = Metrics.create("eventbus");
    if (producerOptions != null &&
        !Strings.isNullOrEmpty(producerOptions.getServers())) {
      SendBackend backend = new SendBackendImpl(producerOptions);
      sendStorage = producerOptions.getSendStorage();
      sendQueue = SendQueue
              .create(producerExecutor, backend, producerOptions.getMaxSendSize(), sendStorage,
                      metrics);
      if (sendStorage != null) {
        Runnable scheduledCommand = () -> {
          List<Event> pending = sendStorage.pendingList();
          pending.forEach(e -> sendQueue.enqueue(e));
        };
        long period = producerOptions.getFetchPendingPeriod();
        scheduledExecutor.scheduleAtFixedRate(scheduledCommand, period, period, TimeUnit
                .MILLISECONDS);
      }
    }
    if (consumerOptions != null &&
        !Strings.isNullOrEmpty(consumerOptions.getServers())) {
      ConsumerBackend consumerBackend =
              new ConsumerBackendImpl(this, consumerOptions, metrics);
      consumerExecutor.submit(consumerBackend);
    }

  }

  @Override
  public boolean send(Event event) {
    boolean checkAndSave = false;
    if (sendStorage != null) {
      checkAndSave = sendStorage.checkAndSave(event);
    }
    boolean result = sendQueue.enqueue(event);
    return checkAndSave || result;
  }

  @Override
  public Map<String, Object> metrics() {
    return metrics.metrics();
  }

  @Override
  public void consumer(String topic, String resource, EventHandler handler) {
    HandlerBinding binding = new HandlerBinding(topic, resource, handler);
    bindings.add(binding);
  }

  @Override
  public void handle(Event event) {
    bindings.stream()
            .filter(b -> b.match(event))
            .forEach(b -> b.eventHandler().handle(event));
  }

  @Override
  public ScheduledExecutorService scheduledExecutor() {
    return scheduledExecutor;
  }

  @Override
  public ExecutorService workerExecutor() {
    return workerExecutor;
  }

  @Override
  public ExecutorService producerExecutor() {
    return producerExecutor;
  }

  @Override
  public ExecutorService consumerExecutor() {
    return consumerExecutor;
  }
}
