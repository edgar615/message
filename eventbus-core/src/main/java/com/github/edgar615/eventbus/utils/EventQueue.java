package com.github.edgar615.eventbus.utils;

import com.github.edgar615.eventbus.event.Event;
import java.util.List;

/**
 * 消息消费的队列.
 *
 * @author Edgar  Date 2018/5/3
 */
public interface EventQueue {

  boolean isFull();

  boolean isLowWaterMark();

  /**
   * 不阻塞，如果没有合适的event，返回null
   * @return
   */
  Event poll();

  /**
   * 如果没有合适的event，阻塞线程
   * @return
   * @throws InterruptedException
   */
  Event dequeue() throws InterruptedException;

  /**
   * 入队
   * @param event
   */
  void enqueue(Event event);

  void enqueue(List<Event> events);

  void complete(Event event);

  int size();
}
