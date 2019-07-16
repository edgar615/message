package com.github.edgar615.eventbus.vertx;

import com.github.edgar615.eventbus.event.Event;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.util.List;

public interface VertxEventBusReadStream {

  void poll(Handler<AsyncResult<List<Event>>> handler);

  void start();

  void close();

  /**
   * 暂停读取消息
   */
  void pause();

  /**
   * 恢复读取消息
   */
  void resume();

  /**
   * 是否暂停
   */
  boolean paused();

}
