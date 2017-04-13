package com.edgar.util.eventbus;

import com.edgar.util.event.Event;
import com.edgar.util.eventbus.metric.DummyMetrics;
import com.edgar.util.eventbus.metric.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

/**
 * 待发送的队列，使用一个的单线程发送.
 *
 * @author Edgar  Date 2017/3/22
 */
class SendQueue {
  private static final Logger LOGGER = LoggerFactory.getLogger(SendQueue.class);

  private final Runnable runnable;

  private final ExecutorService executor;

  private final LinkedList<Event> events = new LinkedList<>();

  private final int maxSize;

  private Metrics metrics = new DummyMetrics();

  private SendStorage storage;

  private final Callback callback = (future) -> {
    Event event = future.event();
    long duration = Instant.now().getEpochSecond() - event.head().timestamp();
    metrics.sendEnd(future.succeeded(), duration);
    if (future.succeeded()) {
      LOGGER.info("======> [{}] [OK] [{}] [{}] [{}] [{}]",
                  event.head().id(),
                  event.head().to(),
                  event.head().action(),
                  Helper.toHeadString(event),
                  Helper.toActionString(event));
    } else {
      LOGGER.error("======> [{}] [FAILED] [{}] [{}] [{}] [{}]",
                   event.head().id(),
                   event.head().to(),
                   event.head().action(),
                   Helper.toHeadString(event),
                   Helper.toActionString(event),
                   future.cause().getMessage());
    }
    mark(event, future.succeeded() ? 1 : 2);
  };

  private boolean running = false;

  private SendQueue(ExecutorService executor, SendBackend backend, int maxSize,
                    SendStorage storage, Metrics metrics) {
    this.executor = executor;
    this.maxSize = maxSize;
    if (metrics != null) {
      this.metrics = metrics;
    }
    this.storage = storage;
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
        if (event.head().duration() > 0) {
          long current = Instant.now().getEpochSecond();
          if (current > event.head().timestamp() + event.head().duration()) {
            LOGGER.info("---|  [{}] [EXPIRE] [{}] [{}] [{}] [{}]",
                        event.head().id(),
                        event.head().to(),
                        event.head().action(),
                        Helper.toHeadString(event),
                        Helper.toActionString(event));

            mark(event, 3);
          } else {
            send(backend, metrics, event);
          }
        } else {
          send(backend, metrics, event);
        }
      }
    };
  }

  public static SendQueue create(ExecutorService producerExecutor, SendBackend backend, int maxSize,
                                 SendStorage storage,
                                 Metrics metrics) {
    return new SendQueue(producerExecutor, backend, maxSize, storage, metrics);
  }

  public boolean enqueue(Event event) {
    synchronized (events) {
      if (events.size() >= maxSize) {
        return false;
      }
      events.add(event);
      metrics.sendEnqueue();
      if (!running) {
        running = true;
        executor.execute(runnable);
      }
      return true;
    }
  }

  private void mark(Event event, int status) {
    try {
      if (storage != null && storage.shouldStorage(event)) {
        storage.mark(event, status);
      }
    } catch (Exception e) {
      LOGGER.warn("---| [{}] [FAILED] [mark:{}] [{}]",
                  event.head().id(),
                  status,
                  e.getMessage());
    }
  }

  private void send(SendBackend backend, Metrics metrics, Event event) {
    backend.send(event)
            .setCallback(callback);
    metrics.sendStart();
  }

}
