package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.concurrent.NamedThreadFactory;
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

  private final EventQueue eventQueue;

  protected Function<Event, Boolean> blackListFilter = event -> false;

  private int maxQuota;

  EventConsumerImpl(ConsumerOptions options) {
    this.metrics = options.getMetrics();
    this.workerExecutor = Executors.newFixedThreadPool(options.getWorkerPoolSize(),
                                                       NamedThreadFactory.create
                                                               ("eventbus-worker"));
    if (options.getIdentificationExtractor() == null) {
      eventQueue = new DefaultEventQueue(options.getMaxQuota());
    } else {
      eventQueue = new SequentialEventQueue(options.getIdentificationExtractor(),
                                            options.getMaxQuota());
    }
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

  /**
   * 入队，如果入队后队列中的任务数量超过了最大数量，暂停消息的读取
   *
   * @param event
   * @return 如果队列中的长度超过最大数量，返回false
   */
  protected synchronized void enqueue(Event event) {
    //先入队
    eventQueue.enqueue(event);
    //提交任务，这里只是创建了一个runnable交由线程池处理，而这个runnable并不一定真正的处理的是当前的event（根据队列的实现来）
    handle();
  }

  protected synchronized boolean isFull() {
    return eventQueue.size() >= maxQuota;
  }

  protected synchronized void enqueue(List<Event> events) {
    eventQueue.enqueue(events);
    //提交任务
    for (Event event : events) {
      handle();
    }
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


  private void handle() {
    workerExecutor.execute(() -> {
      Event event = null;
      try {
        event = eventQueue.dequeue();
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
//        completeFuture.complete();
      } catch (InterruptedException e) {
        LOGGER.error("---| [Failed]", e);
      } catch (Exception e) {
        LOGGER.error("---| [{}] [ERROR]", event.head().id(), e);
      }
    });
  }

}
