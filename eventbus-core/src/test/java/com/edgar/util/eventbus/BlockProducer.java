package com.edgar.util.eventbus;

import com.edgar.util.event.Event;

import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class BlockProducer extends EventProducerImpl {

  private final int block;

  public BlockProducer(ProducerOptions options, int block) {
    super(options);
    this.block = block;
  }

  @Override
  public EventFuture<Void> sendEvent(Event event) {
    if (block > 0) {
      try {
        TimeUnit.SECONDS.sleep(block);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    EventFuture<Void> future = EventFuture.future(event);
    System.out.println("send:" + event);
    future.complete(null);
    return future;
  }
}