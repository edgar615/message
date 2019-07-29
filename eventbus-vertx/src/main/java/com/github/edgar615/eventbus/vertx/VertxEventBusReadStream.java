package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.util.List;

public interface VertxEventBusReadStream {

  void start();

  void close();

  /**
   * 暂停读取消息
   */
  boolean pause();

  /**
   * 恢复读取消息
   */
  boolean resume();

  /**
   * 是否暂停
   */
  boolean paused();

}
