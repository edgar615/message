package com.github.edgar615.util.eventbus.vertx;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
public interface VertxEventbusKafkaConsumer {
  void pause();

  void resume();

  void close();

  long waitForHandle();
}
