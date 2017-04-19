package com.edgar.util.eventbus;

import com.edgar.util.concurrent.NamedThreadFactory;
import com.edgar.util.concurrent.StripedQueue;
import com.edgar.util.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
public abstract class EventConsumerImpl implements EventConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);

  private final ExecutorService workerExecutor;

  private final BlockedEventChecker checker;

  private final int blockedCheckerMs;

  private final Metrics metrics;

  private final StripedQueue queue;

  private final Partitioner partitioner;

  EventConsumerImpl(ConsumerOptions options) {
    this.metrics = options.getMetrics();
    this.partitioner = options.getPartitioner();
    this.workerExecutor = Executors.newFixedThreadPool(
            options.getWorkerPoolSize(),
            NamedThreadFactory.create("eventbus-worker"));
    this.blockedCheckerMs = options.getBlockedCheckerMs();
    if (options.getBlockedCheckerMs() > 0) {
      ScheduledExecutorService scheduledExecutor =
              Executors.newSingleThreadScheduledExecutor(
                      NamedThreadFactory.create("eventbus-blocker-checker"));
      checker = BlockedEventChecker
              .create(options.getBlockedCheckerMs(),
                      scheduledExecutor);
    } else {
      checker = null;
    }

    if (partitioner == null) {
      queue = null;
    } else {
      queue = new StripedQueue(workerExecutor);
    }

  }

  @Override
  public void consumer(String topic, String resource, EventHandler handler) {
    HandlerRegistration.instance().registerHandler(topic, resource, handler);
  }

  protected void handle(Event event, Runnable before, Runnable after) {
    if (partitioner != null) {
      queue.add(partitioner.partition(event), () -> doHandle(event, before, after));
    } else {
      workerExecutor.submit(() -> doHandle(event, before, after));
    }
  }

  private void doHandle(Event event, Runnable before, Runnable after) {
    long start = Instant.now().getEpochSecond();
    BlockedEventHolder holder = BlockedEventHolder.create(event, blockedCheckerMs);
    if (checker != null) {
      checker.register(holder);
    }
    metrics.consumerStart();
    before.run();
    try {
      List<EventHandler> handlers =
              HandlerRegistration.instance()
                      .getHandlers(event);
      if (handlers == null || handlers.isEmpty()) {
        LOGGER.info("---| [{}] [NO HANDLER]", event.head().id());
      } else {
        for (EventHandler handler : handlers) {
          handler.handle(event);
        }
      }
    } catch (Exception e) {
      LOGGER.error("---| [{}] [Failed]", event.head().id(), e);
    }
    metrics.consumerEnd(Instant.now().getEpochSecond() - start);
    holder.completed();
    after.run();
  }
}
