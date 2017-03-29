package com.edgar.util.eventbus;

import com.edgar.util.concurrent.NamedThreadFactory;
import com.edgar.util.event.Event;
import com.edgar.util.metirc.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final ExecutorService consumeExecutor = Executors.newFixedThreadPool(1);

  private final ScheduledExecutorService scheduledExecutor =
          Executors.newSingleThreadScheduledExecutor(
                  NamedThreadFactory.create("eventbus-schedule"));

  private final SendQueue sendQueue;

  private final SendStorage sendStorage;

  private final Metrics metrics;

  EventbusImpl(EventbusOptions options) {
    metrics = Metrics.create("eventbus");
    sendStorage = options.getSendStorage();
    SendBackend backend = options.getSendBackend();
    sendQueue = SendQueue.create(backend, options.getMaxSendSize(), sendStorage, metrics);
    if (sendStorage != null) {
      Runnable scheduledCommand = () -> {
        List<Event> pending = sendStorage.pendingList();
        pending.forEach(e -> sendQueue.enqueue(e));
      };
      long period = options.getFetchPendingPeriod();
      scheduledExecutor.scheduleAtFixedRate(scheduledCommand, period, period, TimeUnit
              .MILLISECONDS);
    }

//    ConsumerRunnable runnable = new ConsumerRunnable();
//    runnable.setClientId(options.getId());
//    runnable.setGroupId(options.getGroup());
//    runnable.setKafkaConnect(options.getServers());
//    options.getConsumerTopics().forEach(t -> {
//      runnable.addTopic(t);
//    });
//    runnable.setStartingOffset(-1);
//    consumeExecutor.execute(runnable);
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


}
