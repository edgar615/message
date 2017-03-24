package com.edgar.util.eventbus;

import com.edgar.util.eventbus.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 待发送的队列，使用一个的单线程发送.
 *
 * @author Edgar  Date 2017/3/22
 */
public class SendedQueue {
  private static final Logger LOGGER = LoggerFactory.getLogger(SendedQueue.class);

  private final Runnable runnable;

  private final AtomicInteger succeed = new AtomicInteger();

  private final AtomicInteger failed = new AtomicInteger();

  private final AtomicInteger processed = new AtomicInteger();

  private final Callback callback = (future) -> {
    Event event = future.event();
    processed.decrementAndGet();
    if (future.succeeded()) {
      succeed.incrementAndGet();
      LOGGER.info("======> [{}] [OK] [{}] [{}]",
                  event.head().id(),
                  event.head(),
                  event.action());
    } else {
      failed.incrementAndGet();
      LOGGER.error("======> [{}] [FAILED] [{}] [{}] [{}]",
                   event.head().id(),
                   event.head(),
                   event.action(),
                   future.cause().getMessage());
    }
  };

  private final ExecutorService executor = Executors.newFixedThreadPool(1);

  private final LinkedList<Event> events = new LinkedList<>();

  private final int maxSize;

  private boolean running = false;

  private SendedQueue(EventSendAction action, int maxSize) {
    this.maxSize = maxSize;
    this.runnable = () -> {
      for (; ; ) {
        final Event event;
        synchronized (events) {
          event = events.poll();
          if (event == null) {
            running = false;
            return;
          }
        }
        action.send(event)
                .setCallback(callback);
        processed.incrementAndGet();
      }
    };
  }

  public static SendedQueue create(EventSendAction action, int maxSize) {
    return new SendedQueue(action, maxSize);
  }

  public void enqueue(Event event) {
    synchronized (events) {
      if (events.size() > maxSize) {
        throw new EventbusException("The sended queue is full");
      }
      events.add(event);
      if (!running) {
        running = true;
        executor.execute(runnable);
      }
    }
  }

}
