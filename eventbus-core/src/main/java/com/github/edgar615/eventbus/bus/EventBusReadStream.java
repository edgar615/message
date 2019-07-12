package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;
import java.util.List;

public interface EventBusReadStream {

  List<Event> poll();

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
