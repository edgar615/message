package com.github.edgar615.message.vertx.kafka;

import static net.logstash.logback.marker.Markers.append;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.utils.MessageSerDe;
import com.github.edgar615.message.utils.LoggingMarker;
import com.github.edgar615.message.vertx.VertxMessageWriteStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class VertxKafkaMessageWriteStream implements VertxMessageWriteStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxKafkaMessageWriteStream.class);

  private final KafkaProducer<String, String> producer;

  public VertxKafkaMessageWriteStream(Vertx vertx, KafkaWriteOptions options) {
    this.producer = KafkaProducer.create(vertx, options.getConfigs());
  }

  @Override
  public void send(Message message, Handler<AsyncResult<Message>> resultHandler) {
    LOGGER.info(LoggingMarker.getLoggingMarker(message, false), "waiting for send");

    String source = null;
    try {
      source = MessageSerDe.serialize(message);
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }

    KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create(message.header().to(), source);
    producer.send(record, done -> {
      if (done.succeeded()) {
        RecordMetadata recordMetadata = done.result();
        Marker messageMarker =
            append("traceId", message.header().id())
                .and(append("topic", recordMetadata.getTopic()))
                .and(append("partition", recordMetadata.getPartition()))
                .and(append("offset", recordMetadata.getOffset()));
        LOGGER.info(messageMarker, "write to kafka");
        resultHandler.handle(Future.succeededFuture(message));
      } else {
        LOGGER.info(LoggingMarker.getIdLoggingMarker(message.header().id()), "write to kafka failed");
        resultHandler.handle(Future.failedFuture(done.cause()));
      }

    });
  }

  @Override
  public void close() {
    this.producer.close();
  }
}
