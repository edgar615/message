package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class BlockProducer extends EventProducerImpl {

  private final int block;

  public BlockProducer(ProducerOptions options, int block) {
    this(options, null, block);
  }

  public BlockProducer(ProducerOptions options, ProducerStorage storage, int block) {
    super(options, storage);
    this.block = block;
  }

  @Override
  public EventFuture sendEvent(Event event) {
    if (block > 0) {
      try {
        TimeUnit.SECONDS.sleep(block);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    EventFuture future = EventFuture.future(event);
    System.out.println("send:" + event);
    future.complete();
    return future;
  }
}
