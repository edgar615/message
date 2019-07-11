package com.github.edgar615.eventbus.kafka;

import static net.logstash.logback.marker.Markers.append;

import com.github.edgar615.eventbus.bus.EventBusWriteStream;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.EventSerDe;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class KafkaEventBusWriteStream implements EventBusWriteStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventBusWriteStream.class);

  private final Producer<String, String> producer;

  public KafkaEventBusWriteStream(Map<String, Object> configs) {
    this.producer = new KafkaProducer<>(configs);
  }

  @Override
  public CompletableFuture<Event> send(Event event) {
    CompletableFuture<Event> future = new CompletableFuture<>();
    String source = null;
    try {
      source = EventSerDe.serialize(event);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    ProducerRecord<String, String> record =
        new ProducerRecord<>(event.head().to(), source);
    producer.send(record, (metadata, exception) -> {
      if (exception == null) {
        Marker messageMarker =
            append("traceId", event.head().id())
                .and(append("topic", metadata.topic()))
                .and(append("topic", metadata.topic()))
                .and(append("partition", metadata.partition()))
                .and(append("offset", metadata.offset()));
        LOGGER.info(messageMarker, "write to kafka");
        future.complete(event);
      } else {
        future.completeExceptionally(exception);
      }
    });

    return null;
  }

  @Override
  public void close() {
    try {
      producer.close(10, TimeUnit.SECONDS);
    } catch (Exception unexpected) {
      LOGGER.warn("Ignored unexpected exception in producer shutdown", unexpected);
    }

    LOGGER.info("shutdown kafka producer");
    producer.close();
  }
}
