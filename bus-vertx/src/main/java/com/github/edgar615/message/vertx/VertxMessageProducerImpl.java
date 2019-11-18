package com.github.edgar615.message.vertx;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.utils.LoggingMarker;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VertxMessageProducerImpl implements VertxMessageProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxMessageProducer.class);

  private final Vertx vertx;

  private final VertxMessageWriteStream writeStream;

  private final VertxMessageProducerRepository producerRepository;

  VertxMessageProducerImpl(Vertx vertx,
      VertxMessageWriteStream writeStream,
      VertxMessageProducerRepository producerRepository) {
    this.vertx = vertx;
    this.writeStream = writeStream;
    this.producerRepository = producerRepository;
  }

  @Override
  public void send(Message message, Handler<AsyncResult<Message>> resultHandler) {
    LOGGER.info(LoggingMarker.getLoggingMarker(message, false), "waiting for send");
    String id = message.header().id();
    writeStream.send(message, ar -> {
      if (ar.succeeded()) {
        LOGGER.info(LoggingMarker.getIdLoggingMarker(id), "send succeed");
      } else {
        LOGGER.error(LoggingMarker.getIdLoggingMarker(id), "send failed", ar.cause().getMessage());
      }
      resultHandler.handle(ar);
    });
  }

  @Override
  public void save(Message message, Handler<AsyncResult<Void>> resultHandler) {
    if (producerRepository == null) {
      throw new UnsupportedOperationException("required repository");
    }
    producerRepository.insert(message, ar -> {
      if (ar.succeeded()) {
        LOGGER.info(LoggingMarker.getLoggingMarker(message, false), "write to db, waiting for send");
      } else {
        LOGGER.error(LoggingMarker.getLoggingMarker(message, false), "write to db failed");
      }
      resultHandler.handle(ar);
    });
  }

  @Override
  public void close() {
    LOGGER.info("close producer");
  }

  @Override
  public void start() {
  }

  @Override
  public int waitForSend() {
    return 0;
  }
}
