package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.concurrent.NamedThreadFactory;
import com.github.edgar615.util.concurrent.StripedQueue;
import com.github.edgar615.util.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiPredicate;
import java.util.function.Function;

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

  private Partitioner partitioner;

  protected Function<Event, Boolean> blackListFilter = event -> false;

  EventConsumerImpl(ConsumerOptions options) {
    this.metrics = options.getMetrics();
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
    queue = new StripedQueue(workerExecutor);
  }

  public EventConsumerImpl setPartitioner(Partitioner partitioner) {
    this.partitioner = partitioner;
    return this;
  }

  public EventConsumerImpl setBlackListFilter(
          Function<Event, Boolean> filter) {
    if (filter== null) {
      blackListFilter = e -> false;
    } else {
      blackListFilter = filter;
    }
    return this;
  }

  @Override
  public void consumer(BiPredicate<String, String> predicate, EventHandler handler) {
    HandlerRegistration.instance().registerHandler(predicate, handler);
  }

  @Override
  public void consumer(String topic, String resource, EventHandler handler) {
    final BiPredicate<String, String> predicate = (t, r) -> {
      boolean topicMatch = true;
      if (topic != null) {
        topicMatch = topic.equals(t);
      }
      boolean resourceMatch = true;
      if (resource != null) {
        resourceMatch = resource.equals(r);
      }
      return topicMatch && resourceMatch;
    };
    consumer(predicate, handler);
  }

  protected void handle(Event event, Runnable before, Runnable after) {
    if (partitioner != null) {
      queue.add(partitioner.partition(event), () -> doHandle(event, before, after));
    } else {
      workerExecutor.submit(() -> doHandle(event, before, after));
    }
  }

  private void doHandle(Event event, Runnable before, Runnable after) {
    try {
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
    } catch (Exception e) {
      LOGGER.error("---| [{}] [ERROR]", event.head().id(), e);
    }
  }
}
