package com.edgar.util.eventbus;

import com.edgar.util.event.Event;
import com.edgar.util.eventbus.EventFuture;
import com.edgar.util.eventbus.SendBackend;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class RoundRobinSendBackend implements SendBackend {

  private final AtomicInteger seq = new AtomicInteger(0);

  private final int block;

  public RoundRobinSendBackend(int block) {this.block = block;}

  @Override
  public EventFuture<Void> send(Event event) {
    if (block > 0) {
      try {
        TimeUnit.SECONDS.sleep(block);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    EventFuture<Void> future = EventFuture.future(event);
    if (seq.getAndIncrement() % 2 == 0) {
      future.complete(null);
    } else {
      future.fail(new RuntimeException("failed"));
    }
    return future;
  }
}
