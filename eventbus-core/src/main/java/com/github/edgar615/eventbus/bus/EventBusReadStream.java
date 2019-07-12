package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;
import java.util.List;

/**
 * 内部维护一个队列用来保存待处理的消息。达到最大值之后需要暂停从MQ读取消息，避免内存溢出
 */
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
