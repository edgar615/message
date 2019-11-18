package com.github.edgar615.message.utils;

import com.github.edgar615.message.core.Message;
import java.util.List;

/**
 * 消息消费的队列.
 *
 * @author Edgar  Date 2018/5/3
 */
public interface MessageQueue {

  boolean isFull();

  boolean isLowWaterMark();

  /**
   * 不阻塞，如果没有合适的event，返回null
   * @return
   */
  Message poll();

  /**
   * 如果没有合适的event，阻塞线程
   * @return
   * @throws InterruptedException
   */
  Message dequeue() throws InterruptedException;

  /**
   * 入队
   * @param message
   */
  void enqueue(Message message);

  void enqueue(List<Message> messages);

  void complete(Message message);

  int size();
}
