package com.github.edgar615.eventbus.kafka.vertx;

import static net.logstash.logback.marker.Markers.append;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.EventSerDe;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import com.github.edgar615.eventbus.vertx.VertxEventBusWriteStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class VertxKafkaEventBusWriteStream implements VertxEventBusWriteStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxKafkaEventBusWriteStream.class);

  private final KafkaProducer<String, String> producer;

  public VertxKafkaEventBusWriteStream(Vertx vertx, Map<String, String> config) {
    Map<String, String> configs = new HashMap<>(config);
    config.put("key.serializer", StringSerializer.class.getName());
    config.put("value.serializer", StringSerializer.class.getName());
    this.producer = KafkaProducer.create(vertx, config);

  }

  @Override
  public void send(Event event, Handler<AsyncResult<Event>> resultHandler) {
    LOGGER.info(LoggingMarker.getLoggingMarker(event, false), "waiting for send");

    String source = null;
    try {
      source = EventSerDe.serialize(event);
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }

    KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create(event.head().to(), source);
    producer.send(record, done -> {
      if (done.succeeded()) {
        RecordMetadata recordMetadata = done.result();
        Marker messageMarker =
            append("traceId", event.head().id())
                .and(append("topic", recordMetadata.getTopic()))
                .and(append("partition", recordMetadata.getPartition()))
                .and(append("offset", recordMetadata.getOffset()));
        LOGGER.info(messageMarker, "write to kafka");
        resultHandler.handle(Future.succeededFuture(event));
      } else {
        LOGGER.info(LoggingMarker.getIdLoggingMarker(event.head().id()), "write to kafka failed");
        resultHandler.handle(Future.failedFuture(done.cause()));
      }

    });
  }

  @Override
  public void close() {
    this.producer.close();
  }
}
