package com.github.edgar615.message.redis;

import static net.logstash.logback.marker.Markers.append;

import com.github.edgar615.message.bus.MessageWriteStream;
import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.utils.MessageSerDe;
import com.github.edgar615.message.utils.LoggingMarker;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class RedisMessageWriteStream implements MessageWriteStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageWriteStream.class);

  private final RedisClient redisClient;

  private StatefulRedisConnection<String, String> connection;

  private RedisStreamAsyncCommands<String, String> streamCommands;

  public RedisMessageWriteStream(RedisClient redisClient) {
    this.redisClient = redisClient;
  }


  @Override
  public void start() {
    this.connection = redisClient.connect();
    this.streamCommands = connection.async();
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

    Map<String, String> body =  new HashMap<>();
    body.put("message", source);
    streamCommands.xadd(message.header().to(), body)
        .thenAccept(id -> {
          Marker messageMarker =
              append("traceId", message.header().id())
                  .and(append("topic", message.header().to()))
                  .and(append("id", id));
          LOGGER.info(messageMarker, "write to redis");
          future.complete(message);
        })
        .exceptionally(throwable -> {
          LOGGER.info(LoggingMarker.getIdLoggingMarker(message.header().id()), "write to redis failed");
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
