package com.github.edgar615.message.utils;

import com.github.edgar615.message.core.Message;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事件的队列，队列的长度有consumer控制，入队不阻塞，出队阻塞
 */
public class SequentialMessageQueue implements MessageQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueue.class);

  /**
   * 任务列表
   */
  private final LinkedList<Message> elements = new LinkedList<>();

  /**
   * 消息注册表，用于确保同一个设备只有一个事件在执行
   */
  private final Set<String> registry = new ConcurrentSkipListSet<>();

  private final int limit;

  private Message takeElement;

  private Function<Message, String> identificationExtractor;

  public static SequentialMessageQueue create(Function<Message, String> identificationExtractor,
      int limit) {
    return new SequentialMessageQueue(identificationExtractor, limit);
  }

  private SequentialMessageQueue(Function<Message, String> identificationExtractor, int limit) {
    Objects.requireNonNull(identificationExtractor);
    this.identificationExtractor = identificationExtractor;
    this.limit = limit;
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
  public synchronized Message poll() {
    if (elements.isEmpty() || takeElement == null) {
      return null;
    }
    return taskNextElement();

  }

  @Override
  public synchronized Message dequeue() throws InterruptedException {
    //如果队列为空，或者下一个出队元素为null，阻塞出队
    while (elements.isEmpty() || takeElement == null) {
      wait();
    }
    //从队列中删除元素
    return taskNextElement();
  }

  @Override
  public synchronized void enqueue(Message message) {
    //唤醒等待出队的线程，如果队空或者下一个出队元素为null，说明可能会有出队线程在等待唤醒
    if (elements.isEmpty() || takeElement == null) {
      //唤醒出队
      notifyAll();
    }
    elements.add(message);
    //计算下一个可以出队的元素
    if (!registry.contains(extractId(message))
        && takeElement == null) {
      takeElement = message;
    }
    LOGGER.debug(LoggingMarker.getIdLoggingMarker(message.header().id()), "enqueue");
  }

  @Override
  public synchronized void enqueue(List<Message> messages) {
    //唤醒等待出队的线程，如果队空或者下一个出队元素为null，说明可能会有出队线程在等待唤醒
    if (elements.isEmpty() || takeElement == null) {
      //唤醒出队
      notifyAll();
    }
    elements.addAll(messages);
    //计算下一个可以出队的元素
    for (Message message : messages) {
      if (!registry.contains(extractId(message))
          && takeElement == null) {
        takeElement = message;
        break;
      }
    }
    if (LOGGER.isDebugEnabled()) {
      messages.forEach(e -> LOGGER.debug(LoggingMarker.getIdLoggingMarker(e.header().id()), "enqueue"));
    }
  }

  @Override
  public synchronized void complete(Message message) {
    if (takeElement == null) {
      notifyAll();
    }
    registry.remove(extractId(message));
    takeElement = next();
  }

  @Override
  public synchronized int size() {
    return elements.size();
  }

  private Message taskNextElement() {//从队列中删除元素
    Message x = takeElement;
    elements.remove(x);
    //将元素加入注册表
    registry.add(extractId(x));
    //重新计算下一个可以出队的元素
    takeElement = next();
    LOGGER.debug(LoggingMarker.getIdLoggingMarker(x.header().id()), "dequeue");
    return x;
  }

  private String extractId(Message message) {
    String id = identificationExtractor.apply(message);
    return id == null ? "unkown" : id;
  }

  private Message next() {
    Message next = null;
    for (Message message : elements) {
      if (!registry.contains(extractId(message))) {
        next = message;
        break;
      }
    }
    return next;
  }

}
