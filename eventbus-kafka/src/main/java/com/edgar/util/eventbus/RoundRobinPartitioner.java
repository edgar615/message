package com.edgar.util.eventbus;

import com.edgar.util.event.Event;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/4/14.
 *
 * @author Edgar  Date 2017/4/14
 */
public class RoundRobinPartitioner implements Partitioner {
  private final AtomicInteger seq = new AtomicInteger(0);

  private final int partitionNum;

  public RoundRobinPartitioner(int partitionNum) {this.partitionNum = partitionNum;}

  @Override
  public int partition(Event event) {
    int index = Math.abs(seq.getAndIncrement());
    return index % partitionNum;
  }
}
