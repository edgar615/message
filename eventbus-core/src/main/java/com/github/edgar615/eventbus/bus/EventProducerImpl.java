package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.dao.EventProducerDao;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EventProducerImpl implements EventProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventProducer.class);

  private final EventProducerDao eventProducerDao;

  private final EventBusWriteStream writeStream;

  private final ProducerOptions options;

  EventProducerImpl(ProducerOptions options, EventBusWriteStream writeStream,
      EventProducerDao eventProducerDao) {
    this.options = options;
    this.writeStream = writeStream;
    this.eventProducerDao = eventProducerDao;
  }

  @Override
  public void start() {
    LOGGER.info("start producer");
  }

  @Override
  public CompletableFuture<Event> send(Event event) {
    LOGGER.info(LoggingMarker.getLoggingMarker(event, false), "waiting for send");
    CompletableFuture<Event> future = new CompletableFuture<>();
    String id = event.head().id();
    writeStream.send(event).thenAccept(e -> {
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
  public void save(Event event) {
    if (eventProducerDao == null) {
      throw new UnsupportedOperationException("required dao");
    }
    eventProducerDao.insert(event);
    LOGGER.info(LoggingMarker.getLoggingMarker(event, false),"write to db, waiting for send");
  }

  @Override
  public void close() {
    LOGGER.info("close producer");
  }

  @Override
  public Map<String, Object> metrics() {
    return null;
  }

  @Override
  public long waitForSend() {
    return 0;
  }
}
