package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class RoundRobinProducer extends EventProducerImpl {

  private final AtomicInteger seq = new AtomicInteger(0);

  private final int block;

  public RoundRobinProducer(ProducerOptions options, ProducerStorage storage, int block) {
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
    if (seq.getAndIncrement() % 2 == 0) {
      future.complete();
    } else {
      future.fail(new RuntimeException("failed"));
    }
    return future;
  }
}
