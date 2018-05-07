package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;

/**
 * 事件的队列，队列的长度有consumer控制，入队不阻塞，出队阻塞
 */
public class SequentialEventQueue implements EventQueue {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventQueue.class);

  /**
   * 任务列表
   */
  private final LinkedList<Event> elements = new LinkedList<>();

  /**
   * 消息注册表，用于确保同一个设备只有一个事件在执行
   */
  private final Set<String> registry = new ConcurrentSkipListSet<>();

  private final int limit;

  private Event takeElement;

  private Function<Event, String> identificationExtractor;

  public SequentialEventQueue(Function<Event, String> identificationExtractor, int limit) {
    Objects.requireNonNull(identificationExtractor);
    this.identificationExtractor = identificationExtractor;
    this.limit = limit;
  }

  @Override
  public synchronized Event poll() {
    if (elements.isEmpty() || takeElement == null) {
      return null;
    }
    return taskNextElement();

  }

  @Override
  public synchronized Event dequeue() throws InterruptedException {
    //如果队列为空，或者下一个出队元素为null，阻塞出队
    while (elements.isEmpty() || takeElement == null) {
      wait();
    }
    //从队列中删除元素
    return taskNextElement();
  }

  @Override
  public synchronized void enqueue(Event event) {
    //唤醒等待出队的线程，如果队空或者下一个出队元素为null，说明可能会有出队线程在等待唤醒
    if (elements.isEmpty() || takeElement == null) {
      //唤醒出队
      notifyAll();
    }
    elements.add(event);
    //计算下一个可以出队的元素
    if (!registry.contains(extractId(event))
        && takeElement == null) {
      takeElement = event;
    }
    Log.create(LOGGER)
            .setLogType("eventbus")
            .setEvent("enqueue")
            .setTraceId(event.head().id())
            .debug();
  }

  @Override
  public synchronized void enqueue(List<Event> events) {
    //唤醒等待出队的线程，如果队空或者下一个出队元素为null，说明可能会有出队线程在等待唤醒
    if (elements.isEmpty() || takeElement == null) {
      //唤醒出队
      notifyAll();
    }
    elements.addAll(events);
    //计算下一个可以出队的元素
    for (Event event : events) {
      if (!registry.contains(extractId(event))
          && takeElement == null) {
        takeElement = event;
        break;
      }
    }
    if (LOGGER.isDebugEnabled()) {
      events.forEach(e -> Log.create(LOGGER)
              .setLogType("eventbus")
              .setEvent("enqueue")
              .setTraceId(e.head().id())
              .debug());
    }
  }

  @Override
  public synchronized void complete(Event event) {
    if (takeElement == null) {
      notifyAll();
    }
    registry.remove(extractId(event));
    takeElement = next();
  }

  @Override
  public synchronized int size() {
    return elements.size();
  }

  private Event taskNextElement() {//从队列中删除元素
    Event x = takeElement;
    elements.remove(x);
    //将元素加入注册表
    registry.add(extractId(x));
    //重新计算下一个可以出队的元素
    takeElement = next();
    Log.create(LOGGER)
            .setLogType("eventbus")
            .setEvent("dequeue")
            .setTraceId(x.head().id())
            .debug();
    return x;
  }

  private String extractId(Event event) {
    String id = identificationExtractor.apply(event);
    return id == null ? "unkown" : id;
  }

  private Event next() {
    Event next = null;
    for (Event event : elements) {
      if (!registry.contains(extractId(event))) {
        next = event;
        break;
      }
    }
    return next;
  }

}
