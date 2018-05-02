package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.eventbus.IdentificationExtractor;

import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 从消息服务器读取的消息.
 *
 * @author Edgar  Date 2017/11/3
 */
public class SequentialQueue implements EventQueue {
  /**
   * 任务列表
   */
  private final LinkedList<Event> events = new LinkedList<>();

  /**
   * 消息注册表，用于确保同一个设备只有一个事件在执行
   */
  private final Set<String> registry = new ConcurrentSkipListSet<>();

  private final IdentificationExtractor extractor;

  private int limit = Integer.MAX_VALUE;

  public SequentialQueue(IdentificationExtractor extractor, int limit) {
    this.extractor = extractor;
    this.limit = limit;
  }

  public SequentialQueue(IdentificationExtractor extractor) {
    this.extractor = extractor;
  }

  /**
   * 取出一个可以执行的任务
   *
   * @return
   */
  public synchronized Event dequeue() {
    if (events.isEmpty()) {
      return null;
    }
    for (Event event : events) {
      String deviceId = extractor.apply(event);
      if (!registry.contains(deviceId)) {
        events.remove(event);
        registry.add(deviceId);
        return event;
      }
    }
    return null;
  }

  @Override
  public synchronized boolean enqueue(Event event) {
    events.add(event);
    if (this.events.size() >= limit) {
      return false;
    }
    return true;
  }

  public synchronized void complete(Event event) {
    registry.remove(extractor.apply(event));
  }

  public synchronized int size() {
    return events.size();
  }
}
