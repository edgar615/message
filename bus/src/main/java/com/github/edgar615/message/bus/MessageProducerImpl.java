package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.MessageProducerRepository;
import com.github.edgar615.message.utils.LoggingMarker;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MessageProducerImpl implements MessageProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducer.class);

  private final MessageProducerRepository messageProducerRepository;

  private final MessageWriteStream writeStream;

  private final ProducerOptions options;

  MessageProducerImpl(ProducerOptions options, MessageWriteStream writeStream,
      MessageProducerRepository messageProducerRepository) {
    this.options = options;
    this.writeStream = writeStream;
    this.messageProducerRepository = messageProducerRepository;
  }

  @Override
  public void start() {
    LOGGER.info("start producer");
  }

  @Override
  public CompletableFuture<Message> send(Message message) {
    LOGGER.info(LoggingMarker.getLoggingMarker(message, false), "waiting for send");
    CompletableFuture<Message> future = new CompletableFuture<>();
    String id = message.header().id();
    writeStream.send(message).thenAccept(e -> {
      LOGGER.info(LoggingMarker.getIdLoggingMarker(id), "send succeed");
      future.complete(e);
    }).exceptionally(throwable -> {
      LOGGER.error(LoggingMarker.getIdLoggingMarker(id), "send failed", throwable.getMessage());
      future.completeExceptionally(throwable);
      return null;
    });
    return future;
  }

  @Override
  public void save(Message message) {
    if (messageProducerRepository == null) {
      throw new UnsupportedOperationException("required repository");
    }
    messageProducerRepository.insert(message);
    LOGGER.info(LoggingMarker.getLoggingMarker(message, false),"write to db, waiting for send");
  }

  @Override
  public void close() {
    LOGGER.info("close producer");
  }

  @Override
  public long waitForSend() {
    return 0;
  }
}
