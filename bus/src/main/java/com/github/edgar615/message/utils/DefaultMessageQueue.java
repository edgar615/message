package com.github.edgar615.message.utils;

import com.github.edgar615.message.core.Message;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2018/5/3.
 *
 * @author Edgar  Date 2018/5/3
 */
public class DefaultMessageQueue implements MessageQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueue.class);

  /**
   * 任务列表
   */
  private final LinkedList<Message> elements = new LinkedList<>();

  private final int limit;

  public static DefaultMessageQueue create(int limit) {
    return new DefaultMessageQueue(limit);
  }

  private DefaultMessageQueue(int limit) {
    this.limit = limit;
  }

  @Override
  public synchronized Message dequeue() throws InterruptedException {
    while (elements.isEmpty()) {
      wait();
    }
    return taskNextElement();
  }

  @Override
  public synchronized void enqueue(Message message) {
    //唤醒等待出队的线程，如果队空或者下一个出队元素为null，说明可能会有出队线程在等待唤醒
    if (elements.isEmpty()) {
      //唤醒出队
      notifyAll();
    }
    elements.add(message);
    LOGGER.debug(LoggingMarker.getIdLoggingMarker(message.header().id()), "enqueue");
  }

  @Override
  public synchronized void enqueue(List<Message> messages) {
    //唤醒等待出队的线程，如果队空或者下一个出队元素为null，说明可能会有出队线程在等待唤醒
    if (elements.isEmpty()) {
      //唤醒出队
      notifyAll();
    }
    elements.addAll(messages);
    if (LOGGER.isDebugEnabled()) {
      messages.forEach(e -> LOGGER.debug(LoggingMarker.getIdLoggingMarker(e.header().id()), "enqueue"));
    }

  }

  @Override
  public synchronized void complete(Message message) {
  }

  @Override
  public synchronized int size() {
    return elements.size();
  }

  @Override
  public synchronized boolean isFull() {
    return elements.size() >= limit;
  }

  @Override
  public synchronized boolean isLowWaterMark() {
    return elements.size() <= limit / 2;
  }

  @Override
  public Message poll() {
    if (elements.isEmpty()) {
      return null;
    }
    return taskNextElement();
  }

  private Message taskNextElement() {
    Message e = next();
    LOGGER.debug(LoggingMarker.getIdLoggingMarker(e.header().id()), "dequeue");
    return e;
  }

  private Message next() {
    return elements.poll();
  }
}
