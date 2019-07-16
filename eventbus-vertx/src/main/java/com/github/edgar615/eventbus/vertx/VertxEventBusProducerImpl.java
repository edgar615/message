package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertxEventBusProducerImpl implements VertxEventBusProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxEventBusProducer.class);

  private final Vertx vertx;

  private final VertxEventBusWriteStream writeStream;

  private final VertxEventProducerRepository producerRepository;

  public VertxEventBusProducerImpl(Vertx vertx,
      VertxEventBusWriteStream writeStream,
      VertxEventProducerRepository producerRepository) {
    this.vertx = vertx;
    this.writeStream = writeStream;
    this.producerRepository = producerRepository;
  }

  @Override
  public void send(Event event, Handler<AsyncResult<Event>> resultHandler) {
    LOGGER.info(LoggingMarker.getLoggingMarker(event, false), "waiting for send");
    String id = event.head().id();
    writeStream.send(event, ar -> {
      if (ar.succeeded()) {
        LOGGER.info(LoggingMarker.getIdLoggingMarker(id), "send succeed");
      } else {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(id), "send failed", ar.cause().getMessage());
      }
      resultHandler.handle(ar);
    });
  }

  @Override
  public void save(Event event, Handler<AsyncResult<Void>> resultHandler) {
    if (producerRepository == null) {
      throw new UnsupportedOperationException("required repository");
    }
    producerRepository.insert(event, ar -> {
      if (ar.succeeded()) {
        LOGGER.info(LoggingMarker.getLoggingMarker(event, false), "write to db, waiting for send");
      } else {
        LOGGER.error(LoggingMarker.getLoggingMarker(event, false), "write to db failed");
      }
      resultHandler.handle(ar);
    });
  }

  @Override
  public void close() {
    LOGGER.info("close producer");
  }

  @Override
  public int waitForSend() {
    return 0;
  }
}
