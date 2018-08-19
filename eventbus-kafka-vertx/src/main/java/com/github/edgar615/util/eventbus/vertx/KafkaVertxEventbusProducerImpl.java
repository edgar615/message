package com.github.edgar615.util.eventbus.vertx;

import com.github.edgar615.util.concurrent.NamedThreadFactory;
import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.eventbus.Helper;
import com.github.edgar615.util.eventbus.KafkaProducerOptions;
import com.github.edgar615.util.exception.DefaultErrorCode;
import com.github.edgar615.util.exception.SystemException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

  //如果使用vertx.executeBlocking来执行发送，可能worker线程会有过多的任务导致发送延迟，所以改用一个线程池来实现
  private final ExecutorService producerExecutor;

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
    this.producerExecutor =
            Executors.newFixedThreadPool(1, NamedThreadFactory.create("eventbus-producer"));
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
      LOGGER.info("[{}] [EP] [expire] [{}] [{}]",
              event.head().id(), Helper.toHeadString(event), Helper.toActionString(event));
      if (producerStorage != null
              && producerStorage.shouldStorage(event)) {
        producerStorage.mark(event, 3,
                Future.<Void>future().completer());
      }
      resultHandler.handle(Future.succeededFuture());
      return;
    }
    Future<Void> future = sendEvent(event);
    future.setHandler(ar -> {
      int remaning = len.decrementAndGet();
      //当队列中的数量小于最大数量的一半时，从持久层中加载队列
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
    });

  }

  private Future<Void> sendEvent(Event event) {
    Future<Void> future = Future.future();
    producerExecutor.submit(() -> {
      len.incrementAndGet();
      metrics.sendStart();
      long start = System.currentTimeMillis();
      ProducerRecord<String, Event> record =
              new ProducerRecord<>(event.head().to(), event);
      producer.send(record, (metadata, exception) -> {
        if (exception == null) {
          LOGGER.info("[{}] [MS] [KAFKA] [OK] [{},{},{}] [{}] [{}]", event.head().id(),
                  metadata.topic(),metadata.partition(),metadata.offset(),
                  Helper.toHeadString(event),
                  Helper.toActionString(event));
          metrics.sendEnd(true, System.currentTimeMillis() - start);
          future.complete();
        } else {
          LOGGER.error("[{}] [MS] [KAFKA] [FAILED] [{}] [{}] [{}]", event.head().id(),
                  event.head().to(),
                  Helper.toHeadString(event),
                  Helper.toActionString(event), exception);
          metrics.sendEnd(false, System.currentTimeMillis() - start);
          future.fail(exception);
        }
      });
    });
    return future;
  }

  @Override
  public void close() {
    producer.close();
    producerExecutor.shutdown();
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
          for (Event event : pending) {
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
