package com.github.edgar615.message.redis;

import com.github.edgar615.message.bus.AbstractMessageReadStream;
import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.MessageConsumerRepository;
import com.github.edgar615.message.utils.MessageQueue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import java.util.List;

public class RedisMessageReadStream extends AbstractMessageReadStream {

  private final RedisClient redisClient;

  private StatefulRedisConnection<String, String> connection;

  private RedisStreamAsyncCommands<String, String> streamCommands;

  public RedisMessageReadStream(RedisClient redisClient, MessageQueue queue,
      MessageConsumerRepository consumerRepository) {
    super(queue, consumerRepository);
    this.redisClient = redisClient;
  }

  @Override
  public List<Message> poll() {
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
