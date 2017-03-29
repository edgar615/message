package com.edgar.util.eventbus.kafka;

import com.edgar.util.event.Event;
import com.edgar.util.eventbus.EventFuture;
import com.edgar.util.eventbus.SendBackend;

import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/29.
 *
 * @author Edgar  Date 2017/3/29
 */
public class BlockSendBackend implements SendBackend {

  private final int block;

  public BlockSendBackend(int block) {this.block = block;}

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
    future.complete(null);
    return future;
  }
}
