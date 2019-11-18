package com.github.edgar615.message.vertx;

public interface VertxMessageReadStream {

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
