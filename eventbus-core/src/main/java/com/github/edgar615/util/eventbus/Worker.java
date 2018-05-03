package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.concurrent.NamedThreadFactory;
import com.github.edgar615.util.event.Event;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Worker {
  private final EventQueue queue;

  private final ExecutorService workerExecutor = Executors.newFixedThreadPool(
          1,
          NamedThreadFactory.create("eventbus-worker"));

  ScheduledExecutorService scheduledExecutor =
          Executors.newSingleThreadScheduledExecutor(
                  NamedThreadFactory.create("eventbus-timer-checker"));


  private AtomicBoolean running = new AtomicBoolean(true);

  public Worker(EventQueue queue) {
    this.queue = queue;
  }

  public void start() throws Exception {
    scheduledExecutor.scheduleAtFixedRate(() -> {
      handleEventInOrder();
    }, 2000, 2000, TimeUnit.MILLISECONDS);
  }

  private void handleEventInOrder() {
    Event event;
    try {
      while ((event = queue.dequeue()) != null) {
        //处理event
        EventFuture<Void> future = EventFuture.future(event);
        handle(event, future);
        final Event finalEvent = event;
        future.setCallback(ar -> {
          queue.complete(finalEvent);
          //尝试将worker的事件处理状态从false修改为true，如果成功说明之前worker没处理事件，开启新一轮处理
          if (running.compareAndSet(false, true)) {
            handleEventInOrder();
          }
        });
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void handle(Event event, EventFuture<Void> completeFuture) {
    workerExecutor.execute(() -> {
      try {
        long start = Instant.now().getEpochSecond();
//      BlockedEventHolder holder = BlockedEventHolder.create(event, blockedCheckerMs);
//      if (checker != null) {
//        checker.register(holder);
//      }
//      metrics.consumerStart();
        try {
          List<EventHandler> handlers =
                  HandlerRegistration.instance()
                          .getHandlers(event);
          if (handlers == null || handlers.isEmpty()) {
//          LOGGER.info("---| [{}] [NO HANDLER]", event.head().id());
          } else {
            for (EventHandler handler : handlers) {
              handler.handle(event);
            }
          }
        } catch (Exception e) {
//        LOGGER.error("---| [{}] [Failed]", event.head().id(), e);
        }
//      metrics.consumerEnd(Instant.now().getEpochSecond() - start);
//      holder.completed();
        completeFuture.complete();
      } catch (Exception e) {
//      LOGGER.error("---| [{}] [ERROR]", event.head().id(), e);
      }
    });
  }

}
