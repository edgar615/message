package com.github.edgar615.eventbus.redis;

import com.github.edgar615.eventbus.bus.AbstractEventBusReadStream;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.repository.EventConsumerRepository;
import com.github.edgar615.eventbus.utils.EventQueue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import java.util.List;

public class RedisEventBusReadStream extends AbstractEventBusReadStream {

  private final RedisClient redisClient;

  private StatefulRedisConnection<String, String> connection;

  private RedisStreamAsyncCommands<String, String> streamCommands;

  public RedisEventBusReadStream(RedisClient redisClient, EventQueue queue,
      EventConsumerRepository consumerRepository) {
    super(queue, consumerRepository);
    this.redisClient = redisClient;
  }

  @Override
  public List<Event> poll() {
    return null;
  }

  @Override
  public void start() {
    this.connection = redisClient.connect();
    this.streamCommands = connection.async();
  }

  @Override
  public void close() {
    if (connection != null && connection.isOpen()) {
      connection.close();
    }
//    redisClient.shutdown();
  }
}
