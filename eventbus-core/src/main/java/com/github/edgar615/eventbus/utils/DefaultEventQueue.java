package com.github.edgar615.eventbus.utils;

import com.github.edgar615.eventbus.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Edgar on 2018/5/3.
 *
 * @author Edgar  Date 2018/5/3
 */
public class DefaultEventQueue implements EventQueue {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventQueue.class);

  /**
   * 任务列表
   */
  private final LinkedList<Event> elements = new LinkedList<>();

  private final int limit;

  public DefaultEventQueue(int limit) {
    this.limit = limit;
  }

  @Override
  public synchronized Event dequeue() throws InterruptedException {
    while (elements.isEmpty()) {
      wait();
    }
    return taskNextElement();
  }

  @Override
  public synchronized void enqueue(Event event) {
    //唤醒等待出队的线程，如果队空或者下一个出队元素为null，说明可能会有出队线程在等待唤醒
    if (elements.isEmpty()) {
      //唤醒出队
      notifyAll();
    }
    elements.add(event);
    LOGGER.debug("[{}] [EC] [enqueue]", event.head().id());
  }

  @Override
  public synchronized void enqueue(List<Event> events) {
    //唤醒等待出队的线程，如果队空或者下一个出队元素为null，说明可能会有出队线程在等待唤醒
    if (elements.isEmpty()) {
      //唤醒出队
      notifyAll();
    }
    elements.addAll(events);
    if (LOGGER.isDebugEnabled()) {
      events.forEach(e -> LOGGER.debug("[{}] [EC] [enqueue]", e.head().id()));
    }

  }

  @Override
  public synchronized void complete(Event event) {
  }

  @Override
  public synchronized int size() {
    return elements.size();
  }

  @Override
  public synchronized boolean isFull() {
    return elements.size() >= limit;
  }

  @Override
  public synchronized boolean isLowWaterMark() {
    return elements.size() <= limit / 2;
  }

  @Override
  public Event poll() {
    if (elements.isEmpty()) {
      return null;
    }
    return taskNextElement();
  }

  private Event taskNextElement() {
    Event e = next();
    LOGGER.debug("[{}] [EC] [dequeue]", e.head().id());
    return e;
  }

  private Event next() {
    return elements.poll();
  }
}
