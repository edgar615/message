package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.List;

/**
 * Created by Edgar on 2018/5/3.
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

  void enqueue(Event event);

  void enqueue(List<Event> events);

  void complete(Event event);

  int size();
}
