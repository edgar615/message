package com.github.edgar615.eventbus.redis;

import static net.logstash.logback.marker.Markers.append;

import com.github.edgar615.eventbus.bus.EventBusWriteStream;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.EventSerDe;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class RedisEventBusWriteStream implements EventBusWriteStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventBusWriteStream.class);

  private final RedisClient redisClient;

  private StatefulRedisConnection<String, String> connection;

  private RedisStreamAsyncCommands<String, String> streamCommands;

  public RedisEventBusWriteStream(RedisClient redisClient) {
    this.redisClient = redisClient;
  }


  @Override
  public void start() {
    this.connection = redisClient.connect();
    this.streamCommands = connection.async();
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

    Map<String, String> body =  new HashMap<>();
    body.put("event", source);
    streamCommands.xadd(event.head().to(), body)
        .thenAccept(id -> {
          Marker messageMarker =
              append("traceId", event.head().id())
                  .and(append("topic", event.head().to()))
                  .and(append("id", id));
          LOGGER.info(messageMarker, "write to redis");
          future.complete(event);
        })
        .exceptionally(throwable -> {
          LOGGER.info(LoggingMarker.getIdLoggingMarker(event.head().id()), "write to redis failed");
          future.completeExceptionally(throwable);
          return null;
        });
    return future;
  }

  @Override
  public void close() {
    if (connection != null && connection.isOpen()) {
      connection.close();
    }
//    redisClient.shutdown();
  }

}
