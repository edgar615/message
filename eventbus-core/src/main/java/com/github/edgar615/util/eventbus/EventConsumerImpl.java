package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.concurrent.NamedThreadFactory;
import com.github.edgar615.util.concurrent.StripedQueue;
import com.github.edgar615.util.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
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

  private long maxQuota;

  /**
   * 任务列表
   */
  private final LinkedList<Event> events = new LinkedList<>();

  protected Function<Event, Boolean> blackListFilter = event -> false;

  private volatile boolean pause = false;

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
    maxQuota = options.getMaxQuota();
  }

  public EventConsumerImpl setBlackListFilter(
          Function<Event, Boolean> filter) {
    if (filter == null) {
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

  /**
   * 入队，如果入队后队列中的任务数量超过了最大数量，暂停消息的读取
   *
   * @param event
   * @return 如果队列中的长度超过最大数量，返回false
   */
  protected synchronized boolean enqueue(Event event) {
    events.add(event);

    if (this.events.size() >= maxQuota) {
      pause();
      return false;
    }
    return true;
  }

  private void handle(Event event, EventFuture<Void> completeFuture) {
    workerExecutor.execute(() -> {
      try {
        long start = Instant.now().getEpochSecond();
        BlockedEventHolder holder = BlockedEventHolder.create(event, blockedCheckerMs);
        if (checker != null) {
          checker.register(holder);
        }
        metrics.consumerStart();
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
        completeFuture.complete();
      } catch (Exception e) {
        LOGGER.error("---| [{}] [ERROR]", event.head().id(), e);
      }
    });
  }

  /**
   * 暂停入队
   */
  public void pause() {
    pause = true;
  }

  /**
   * 回复入队
   */
  public void resume() {
    pause = false;
  }

  public void complete(Event event) {
  }

  protected synchronized boolean enqueue(List<Event> events) {
    events.addAll(events);
    if (this.events.size() >= maxQuota) {
      pause();
      return false;
    }
    return true;
  }

}
