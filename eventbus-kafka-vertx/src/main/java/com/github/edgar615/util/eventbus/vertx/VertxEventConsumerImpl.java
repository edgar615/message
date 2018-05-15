package com.github.edgar615.util.eventbus.vertx;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.eventbus.DefaultEventQueue;
import com.github.edgar615.util.eventbus.EventQueue;
import com.github.edgar615.util.eventbus.HandlerRegistration;
import com.github.edgar615.util.eventbus.KafkaConsumerOptions;
import com.github.edgar615.util.eventbus.KafkaReadStream;
import com.github.edgar615.util.log.Log;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
public class VertxEventConsumerImpl implements VertxEventConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxEventConsumerImpl.class);

  private static final String LOG_TYPE = "eventbus-consumer";

  private final Vertx vertx;

  private final KafkaReadStream readStream;

  private final EventQueue eventQueue;

  private final Handler<AsyncResult<Event>> resultHandler;

  public VertxEventConsumerImpl(Vertx vertx, KafkaConsumerOptions options) {
    this.vertx = vertx;
    this.eventQueue = new DefaultEventQueue(100);
    this.readStream = new KafkaReadStream(options, eventQueue);
    this.resultHandler = ar -> {
      eventQueue.complete(ar.result());
      //处理完成后立刻拉取下一个消息
      schedule(0);
    };
  }

  @Override
  public void pause() {
    readStream.pause();
  }

  @Override
  public void resume() {
    readStream.resume();
  }

  @Override
  public void close() {
    readStream.close();
  }

  private void schedule(long delay) {
    if (delay > 0) {
      vertx.setTimer(delay, l -> run(resultHandler));
    } else {
      run(resultHandler);
    }
  }

  private void run(Handler<AsyncResult<Event>> resultHandler) {
    Event event = eventQueue.poll();
    if (event == null) {
      //如果没有事件，等待100毫秒
      schedule(100);
      return;
    }
    List<VertxEventHandler> handlers =
            HandlerRegistration.instance()
                    .getHandlers(event)
                    .stream().filter(h -> h instanceof VertxEventHandler)
                    .map(h -> (VertxEventHandler) h)
                    .collect(Collectors.toList());
    if (handlers == null || handlers.isEmpty()) {
      Log.create(LOGGER)
              .setLogType("eventbus-consumer")
              .setEvent("handle")
              .setTraceId(event.head().id())
              .setMessage("NO HANDLER")
              .warn();
      resultHandler.handle(Future.succeededFuture(event));
    } else {
      List<Future> futures = new ArrayList<>();
      for (VertxEventHandler handler : handlers) {
        Future<Void> future = Future.future();
        futures.add(future);
        handler.handle(event, future);
      }
      CompositeFuture.all(futures)
              .setHandler(ar -> {
                resultHandler.handle(Future.succeededFuture(event));
                if (ar.succeeded()) {
                } else {
                }
              });
    }
  }
}
