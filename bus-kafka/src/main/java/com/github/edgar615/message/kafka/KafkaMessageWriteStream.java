package com.github.edgar615.message.kafka;

import static net.logstash.logback.marker.Markers.append;

import com.github.edgar615.message.bus.MessageWriteStream;
import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.utils.MessageSerDe;
import com.github.edgar615.message.utils.LoggingMarker;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class KafkaMessageWriteStream implements MessageWriteStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageWriteStream.class);

  private final Producer<String, String> producer;

  public KafkaMessageWriteStream(KafkaWriteOptions options) {
    this.producer = new KafkaProducer<>(options.getConfigs());
  }

  @Override
  public CompletableFuture<Message> send(Message message) {
    CompletableFuture<Message> future = new CompletableFuture<>();
    String source = null;
    try {
      source = MessageSerDe.serialize(message);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    ProducerRecord<String, String> record =
        new ProducerRecord<>(message.header().to(), source);
    producer.send(record, (metadata, exception) -> {
      if (exception == null) {
        Marker messageMarker =
            append("traceId", message.header().id())
                .and(append("topic", metadata.topic()))
                .and(append("partition", metadata.partition()))
                .and(append("offset", metadata.offset()));
        LOGGER.info(messageMarker, "write to kafka");
        future.complete(message);
      } else {
        LOGGER.info(LoggingMarker.getIdLoggingMarker(message.header().id()), "write to kafka failed");
        future.completeExceptionally(exception);
      }
    });

    return future;
  }

  @Override
  public void start() {

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
