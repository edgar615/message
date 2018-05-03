package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by Edgar on 2018/5/3.
 *
 * @author Edgar  Date 2018/5/3
 */
public class DefaultEventQueue implements EventQueue {
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
    return next();
  }

  @Override
  public synchronized void enqueue(Event event) {
    //唤醒等待出队的线程，如果队空或者下一个出队元素为null，说明可能会有出队线程在等待唤醒
    if (elements.isEmpty()) {
      //唤醒出队
      notifyAll();
    }
    elements.add(event);
  }

  @Override
  public synchronized void enqueue(List<Event> events) {
    //唤醒等待出队的线程，如果队空或者下一个出队元素为null，说明可能会有出队线程在等待唤醒
    if (elements.isEmpty()) {
      //唤醒出队
      notifyAll();
    }
    elements.addAll(events);
  }

  @Override
  public synchronized void complete(Event event) {
  }

  @Override
  public synchronized int size() {
    return elements.size();
  }

  private Event next() {
    return elements.poll();
  }
}
