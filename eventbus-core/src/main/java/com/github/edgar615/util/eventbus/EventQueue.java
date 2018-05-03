package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.List;

/**
 * Created by Edgar on 2018/5/3.
 *
 * @author Edgar  Date 2018/5/3
 */
public interface EventQueue {

  Event dequeue() throws InterruptedException;

  void enqueue(Event event);

  void enqueue(List<Event> events);

  void complete(Event event);

  int size();
}
