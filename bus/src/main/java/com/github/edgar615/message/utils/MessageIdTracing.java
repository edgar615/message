package com.github.edgar615.message.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2018/7/14.
 *
 * @author Edgar  Date 2018/7/14
 */
public class MessageIdTracing {
  private final String id;

  private final AtomicInteger seq = new AtomicInteger();

  public MessageIdTracing(String id) {this.id = id;}

  public String nextId() {
    return id + "" + seq.incrementAndGet();
  }
}
