package com.github.edgar615.util.eventbus.vertx;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.eventbus.Helper;
import com.github.edgar615.util.eventbus.KafkaProducerOptions;
import com.github.edgar615.util.exception.DefaultErrorCode;
import com.github.edgar615.util.exception.SystemException;
import com.github.edgar615.util.log.Log;
import com.github.edgar615.util.log.LogType;
import com.github.edgar615.util.metrics.DummyMetrics;
import com.github.edgar615.util.metrics.ProducerMetrics;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
class KafkaVertxEventbusProducerImpl implements KafkaVertxEventbusProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaVertxEventbusProducer.class);

  private final Vertx vertx;

  private final Producer<String, Event> producer;

  private final VertxProducerStorage producerStorage;

  private final AtomicInteger len = new AtomicInteger();

  private final long maxQuota;

  private final long fetchPendingPeriod;

  private final ProducerMetrics metrics;

  KafkaVertxEventbusProducerImpl(Vertx vertx, KafkaProducerOptions options) {
    this(vertx, options, null);
  }

  KafkaVertxEventbusProducerImpl(Vertx vertx, KafkaProducerOptions options,
                                 VertxProducerStorage storage) {
    this.vertx = vertx;
    this.maxQuota = options.getMaxQuota();
    this.producer = new KafkaProducer<>(options.toProps());
    this.producerStorage = storage;
    this.fetchPendingPeriod = options.getFetchPendingPeriod();
    this.metrics = createMetrics();
    if (storage != null) {
      schedule(fetchPendingPeriod);
    }
  }

  @Override
  public void send(Event event, Handler<AsyncResult<Void>> resultHandler) {
    String storage = event.head().ext("__storage");
    //持久化的消息，就认为成功，可以直接返回，不在加入发送队列
    if (producerStorage != null && !"1".equals(storage) && producerStorage.shouldStorage(event)) {
      //与调用方在同一个线程处理
      producerStorage.save(event, resultHandler);
      return;
    }
    if (len.get() > maxQuota && !"1".equals(storage)) {
      SystemException exception = SystemException.create(DefaultErrorCode.TOO_MANY_REQ)
              .set("maxQuota", maxQuota);
      resultHandler.handle(Future.failedFuture(exception));
      return;
    }
    long current = Instant.now().getEpochSecond();
    if (event.head().duration() > 0
        && current > event.head().timestamp() + event.head().duration()) {
      Log.create(LOGGER)
              .setLogType("eventbus-producer")
              .setEvent("EXPIRE")
              .setTraceId(event.head().id())
              .setMessage("[{}] [{}] [{}] [{}]")
              .addArg(event.head().to())
              .addArg(event.head().action())
              .addArg(Helper.toHeadString(event))
              .addArg(Helper.toActionString(event))
              .info();
      if (producerStorage != null
          && producerStorage.shouldStorage(event)) {
        producerStorage.mark(event, 3,
                             Future.<Void>future().completer());
      }
      resultHandler.handle(Future.succeededFuture());
      return;
    }

    vertx.executeBlocking(f -> {
                            len.incrementAndGet();
                            metrics.sendStart();
                            long start = System.currentTimeMillis();
                            ProducerRecord<String, Event> record =
                                    new ProducerRecord<>(event.head().to(), event);
                            producer.send(record, (metadata, exception) -> {
                              if (exception == null) {
                                Log.create(LOGGER)
                                        .setLogType(LogType.MS)
                                        .setEvent("kafka")
                                        .setTraceId(event.head().id())
                                        .setMessage("[{},{},{}] [{}] [{}] [{}]")
                                        .addArg(metadata.topic())
                                        .addArg(metadata.partition())
                                        .addArg(metadata.offset())
                                        .addArg(event.head().action())
                                        .addArg(Helper.toHeadString(event))
                                        .addArg(Helper.toActionString(event))
                                        .info();
                                metrics.sendEnd(true, System.currentTimeMillis() - start);
                                f.complete();
                              } else {
                                Log.create(LOGGER)
                                        .setLogType(LogType.MS)
                                        .setEvent("kafka")
                                        .setTraceId(event.head().id())
                                        .setMessage("[{}] [{}] [{}]")
                                        .addArg(event.head().action())
                                        .addArg(Helper.toHeadString(event))
                                        .addArg(Helper.toActionString(event))
                                        .setThrowable(exception)
                                        .error();
                                metrics.sendEnd(false, System.currentTimeMillis() - start);
                                f.fail(exception);
                              }
                            });
                          }, ar ->
                                  vertx.runOnContext(v -> {
                                    int remaning = len.decrementAndGet();
                                    //当队列中的数量小于最大数量的一半时，从持久层中记载队列
                                    if (producerStorage != null && remaning < maxQuota / 2) {
                                      schedule(0);
                                    }
                                    if (ar.failed()) {
                                      if (producerStorage != null
                                          && producerStorage.shouldStorage(event)) {
                                        producerStorage.mark(event, 2,
                                                             Future.<Void>future().completer());
                                      }
                                      resultHandler.handle(Future.failedFuture(ar.cause()));
                                    } else {
                                      if (producerStorage != null
                                          && producerStorage.shouldStorage(event)) {
                                        producerStorage.mark(event, 1,
                                                             Future.<Void>future().completer());
                                      }
                                      resultHandler.handle(Future.succeededFuture());
                                    }
                                  })
    );
  }

  @Override
  public void close() {
    producer.close();
  }

  @Override
  public int waitForSend() {
    return len.get();
  }

  @Override
  public Map<String, Object> metrics() {
    return metrics.metrics();
  }

  private ProducerMetrics createMetrics() {
    ServiceLoader<ProducerMetrics> metrics = ServiceLoader.load(ProducerMetrics.class);
    Iterator<ProducerMetrics> iterator = metrics.iterator();
    if (!iterator.hasNext()) {
      return new DummyMetrics();
    } else {
      return iterator.next();
    }
  }

  private void run() {
    producerStorage.pendingList(ar -> {
      if (ar.succeeded()) {
        List<Event> pending = ar.result();
        //如果queue的中有数据，那么schedule会在回调中执行
        if (pending.isEmpty() && len.get() == 0) {
          schedule(fetchPendingPeriod);
        } else {
          for (Event event: pending) {
            //在消息头加上一个标识符，标明是从存储中读取，不在进行持久化
            event.head().addExt("__storage", "1");
            send(event, Future.<Void>future().completer());
          }
        }
      } else {
        schedule(fetchPendingPeriod);
      }
    });
  }

  private void schedule(long delay) {
    if (delay > 0) {
      vertx.setTimer(delay, l -> run());
    } else {
      run();
    }
  }
}
