package com.github.edgar615.util.eventbus;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2018/7/14.
 *
 * @author Edgar  Date 2018/7/14
 */
public class EventIdTracing {
  private final String id;

  private final AtomicInteger seq = new AtomicInteger();

  public EventIdTracing(String id) {this.id = id;}

  public String nextId() {
    return id + "." + seq.incrementAndGet();
  }
}
