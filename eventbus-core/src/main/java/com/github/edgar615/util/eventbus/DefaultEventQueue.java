package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.log.Log;
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
    Log.create(LOGGER)
            .setLogType("eventbus")
            .setEvent("enqueue")
            .setTraceId(event.head().id())
            .debug();
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
      events.forEach(e -> Log.create(LOGGER)
              .setLogType("eventbus")
              .setEvent("enqueue")
              .setTraceId(e.head().id())
              .debug());
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
  public Event poll() {
    if (elements.isEmpty()) {
      return null;
    }
    return taskNextElement();
  }

  private Event taskNextElement() {
    Event e = next();
    Log.create(LOGGER)
            .setLogType("eventbus")
            .setEvent("dequeue")
            .setTraceId(e.head().id())
            .debug();
    return e;
  }

  private Event next() {
    return elements.poll();
  }
}
