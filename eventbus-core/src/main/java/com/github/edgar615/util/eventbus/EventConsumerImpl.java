package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.concurrent.NamedThreadFactory;
import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.log.Log;
import com.github.edgar615.util.metrics.ConsumerMetrics;
import com.github.edgar615.util.metrics.DummyMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
public abstract class EventConsumerImpl implements EventConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);

  private final ExecutorService workerExecutor;

  private final ScheduledExecutorService scheduledExecutor;

  private final BlockedEventChecker checker;

  private final int blockedCheckerMs;

  private final ConsumerMetrics metrics;

  private EventQueue eventQueue;

  private Function<Event, Boolean> blackListFilter = event -> false;

  private volatile boolean running = false;

  private ConsumerStorage consumerStorage;

  EventConsumerImpl(ConsumerOptions options) {
    this(options, null, null, null);
  }

  EventConsumerImpl(ConsumerOptions options, ConsumerStorage consumerStorage,
                    Function<Event, String> identificationExtractor,
                    Function<Event, Boolean> blackListFilter) {
    this.metrics = createMetrics();
    this.workerExecutor = Executors.newFixedThreadPool(options.getWorkerPoolSize(),
                                                       NamedThreadFactory.create
                                                               ("eventbus-consumer-worker"));
    this.blockedCheckerMs = options.getBlockedCheckerMs();
    if (options.getBlockedCheckerMs() > 0) {
      this.scheduledExecutor =
              Executors.newSingleThreadScheduledExecutor(
                      NamedThreadFactory.create("eventbus-blocker-checker"));
      this.checker = BlockedEventChecker
              .create(options.getBlockedCheckerMs(),
                      scheduledExecutor);
    } else {
      checker = null;
      this.scheduledExecutor = null;
    }
    running = true;
    this.consumerStorage = consumerStorage;
    if (blackListFilter == null) {
      this.blackListFilter = e -> false;
    } else {
      this.blackListFilter = blackListFilter;
    }
    if (identificationExtractor == null) {
      this.eventQueue = new DefaultEventQueue(options.getMaxQuota());
    } else {
      this.eventQueue = new SequentialEventQueue(identificationExtractor, options.getMaxQuota());
    }
  }

  private final void handle() {
    workerExecutor.submit(() -> {
      Event event = null;
      try {
        event = eventQueue.dequeue();
        long start = Instant.now().getEpochSecond();
        BlockedEventHolder holder = BlockedEventHolder.create(event.head().id(), blockedCheckerMs);
        if (checker != null) {
          checker.register(holder);
        }
        metrics.consumerStart();
        doHandle(event);
        holder.completed();
        long duration = Instant.now().getEpochSecond() - start;
        metrics.consumerEnd(duration);
        eventQueue.complete(event);
        Log.create(LOGGER)
                .setLogType("eventbus-consumer")
                .setEvent("complete")
                .setTraceId(event.head().id())
                .addData("duration", duration)
                .info();
      } catch (InterruptedException e) {
        Log.create(LOGGER)
                .setLogType("eventbus-consumer")
                .setEvent("handle")
                .setThrowable(e)
                .error();
//        因此中断一个运行在线程池中的任务可以起到双重效果，一是取消任务，二是通知执行线程线程池正要关闭。如果任务生吞中断请求，则 worker
// 线程将不知道有一个被请求的中断，从而耽误应用程序或服务的关闭
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        Log.create(LOGGER)
                .setLogType("eventbus")
                .setEvent("handle")
                .setTraceId(event.head().id())
                .setThrowable(e)
                .error();
      }
    });
  }

  @Override
  public void close() {
    //关闭消息订阅
    running = false;
    Log.create(LOGGER)
            .setLogType("eventbus-consumer")
            .setEvent("close")
            .addData("remaining", waitForHandle())
            .info();
    workerExecutor.shutdown();
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
    }
  }

  @Override
  public long waitForHandle() {
    ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) workerExecutor;
    return poolExecutor.getTaskCount() - poolExecutor.getCompletedTaskCount();
  }

  @Override
  public Map<String, Object> metrics() {
    return metrics.metrics();
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

  protected EventQueue queue() {
    return eventQueue;
  }

  protected void enqueue(List<Event> events) {
    List<String> consumedEvents = checkConsumed(events);
    List<String> blacklistEvents = checkBlacklist(events, consumedEvents);

    //入队
    List<Event> enqueEvents = events.stream().filter(e -> !consumedEvents.contains(e.head().id()))
            .filter(e -> !blacklistEvents.contains(e.head().id()))
            .collect(Collectors.toList());
    eventQueue.enqueue(enqueEvents);
    //只提交任务不属于黑名单的消息
    for (Event event : enqueEvents) {
      handle();
    }
  }

  /**
   * 入队，如果入队后队列中的任务数量超过了最大数量，暂停消息的读取
   *
   * @param event
   * @return 如果队列中的长度超过最大数量，返回false
   */
  protected void enqueue(Event event) {
    if (consumerStorage != null && consumerStorage.shouldStorage(event)
        && consumerStorage.isConsumed(event)) {
      Log.create(LOGGER)
              .setLogType("event-consumer")
              .setEvent("RepeatedConsumer")
              .setTraceId(event.head().id())
              .info();
      return;
    }
    if (blackListFilter != null && blackListFilter.apply(event)) {
      Log.create(LOGGER)
              .setLogType("event-consumer")
              .setEvent("blacklist")
              .setTraceId(event.head().id())
              .info();
      if (consumerStorage != null && consumerStorage.shouldStorage(event)) {
        consumerStorage.mark(event, 3);
      }
      return;
    }
    //先入队
    eventQueue.enqueue(event);
    //提交任务，这里只是创建了一个runnable交由线程池处理，而这个runnable并不一定真正的处理的是当前的event（根据队列的实现来）
    handle();
  }

  protected boolean isFull() {
    return eventQueue.isFull();
  }

  protected boolean isLowWaterMark() {
    return eventQueue.isLowWaterMark();
  }


  protected int size() {
    return eventQueue.size();
  }

  protected boolean isRunning() {
    return running;
  }

  private void doHandle(Event event) {
    try {
      List<EventHandler> handlers =
              HandlerRegistration.instance()
                      .getHandlers(event);
      if (handlers == null || handlers.isEmpty()) {
        Log.create(LOGGER)
                .setLogType("eventbus-consumer")
                .setEvent("handle")
                .setTraceId(event.head().id())
                .setMessage("NO HANDLER")
                .warn();
      } else {
        for (EventHandler handler : handlers) {
          handler.handle(event);
        }
      }
      if (consumerStorage != null && consumerStorage.shouldStorage(event)) {
        consumerStorage.mark(event, 1);
      }
    } catch (Exception e) {
      Log.create(LOGGER)
              .setLogType("eventbus")
              .setEvent("handle")
              .setTraceId(event.head().id())
              .setThrowable(e)
              .error();
      if (consumerStorage != null && consumerStorage.shouldStorage(event)) {
        consumerStorage.mark(event, 2);
      }
    }
  }

  /**
   * 过滤重复消费
   *
   * @param events
   * @return
   */
  private List<String> checkConsumed(List<Event> events) {
    if (consumerStorage == null) {
      return new ArrayList<>();
    }
    //过滤掉重复消费的
    List<String> ids = (events.stream()
            .filter(e -> consumerStorage.shouldStorage(e))
            .filter(e -> consumerStorage.isConsumed(e))
            .map(e -> e.head().id())
            .collect(Collectors.toList()));
    for (String id : ids) {
      Log.create(LOGGER)
              .setLogType("event-consumer")
              .setEvent("RepeatedConsumer")
              .setTraceId(id)
              .info();
    }
    return ids;
  }

  /**
   * 过滤黑名单
   *
   * @param events
   * @param consumedEvents
   */
  private List<String> checkBlacklist(List<Event> events, List<String> consumedEvents) {
    if (blackListFilter == null) {
      return new ArrayList<>();
    }
    List<Event> blacklistEvents = events.stream().filter(e -> blackListFilter.apply(e))
            .filter(e -> !consumedEvents.contains(e.head().id()))
            .collect(Collectors.toList());
    for (Event event : blacklistEvents) {
      Log.create(LOGGER)
              .setLogType("event-consumer")
              .setEvent("blacklist")
              .setTraceId(event.head().id())
              .info();
      if (consumerStorage != null && consumerStorage.shouldStorage(event)) {
        //标记为黑名单
        consumerStorage.mark(event, 3);
      }
    }
    return blacklistEvents.stream().map(e -> e.head().id()).collect(Collectors.toList());
  }

  private ConsumerMetrics createMetrics() {
    ServiceLoader<ConsumerMetrics> metrics = ServiceLoader.load(ConsumerMetrics.class);
    Iterator<ConsumerMetrics> iterator = metrics.iterator();
    if (!iterator.hasNext()) {
      return new DummyMetrics();
    } else {
      return iterator.next();
    }
  }

}
