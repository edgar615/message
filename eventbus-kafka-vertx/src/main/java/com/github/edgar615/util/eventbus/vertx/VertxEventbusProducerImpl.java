package com.github.edgar615.util.eventbus.vertx;

import com.github.edgar615.util.event.Event;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
public class VertxEventbusProducerImpl implements VertxEventbusProducer {

//  private final Vertx vertx;
//
//  private final KafkaProducerOptions options;
//
////  private final KafkaProducer<String, Event> producer;
//
//  private final long maxQuota;
//
//  private final AtomicInteger pending = new AtomicInteger();

//  public VertxEventbusProducerImpl(Vertx vertx, KafkaProducerOptions options) {
//    this.vertx = vertx;
//    this.options = options;
//    this.maxQuota = options.getMaxQuota();
//    this.producer = KafkaProducer.create(vertx, options.toProps());
//  }
//
//  public void send(Event event) {
//    send(event, null);
//  }
//
//  public void send(Event event, Handler<AsyncResult<RecordMetadata>> handler) {
//    if (pending.incrementAndGet() > maxQuota) {
//      //todo，返回错误
//    }
//    KafkaProducerRecord<String, Event> record
//            = KafkaProducerRecord.create(event.head().to(), event);
//    producer.write(record, ar -> {
//      pending.decrementAndGet();
//      handler.handle(ar);
//    });
//  }
}
