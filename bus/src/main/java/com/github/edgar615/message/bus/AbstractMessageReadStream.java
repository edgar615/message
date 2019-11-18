package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.MessageConsumerRepository;
import com.github.edgar615.message.utils.MessageQueue;
import com.github.edgar615.message.utils.LoggingMarker;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMessageReadStream implements MessageReadStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageReadStream.class);

  private final MessageQueue queue;

  private volatile boolean pause = false;

  private final AtomicInteger pauseCount = new AtomicInteger();

  private volatile long latestPaused = 0L;

  private final MessageConsumerRepository consumerRepository;

  public AbstractMessageReadStream(MessageQueue queue, MessageConsumerRepository consumerRepository) {
    this.queue = queue;
    this.consumerRepository = consumerRepository;
  }

  @Override
  public void pause() {
    pause = true;
    int count = pauseCount.incrementAndGet();
    latestPaused = System.currentTimeMillis();
    LOGGER.info("pause poll, pause times:{}", count);
  }

  @Override
  public void resume() {
    pause = false;
    LOGGER.info("resume poll, paused {}ms", System.currentTimeMillis() - latestPaused);
  }

  @Override
  public boolean paused() {
    return pause;
  }

  /**
   * 对于从MQ读取消息的主线程应该在while循环中调用这个pollAndEnqueue，否则可能会出现无法从暂停状态恢复的问题
   */
  public final int pollAndEnqueue() {
    List<Message> messages = poll();
    if (messages.size() > 0) {
      LOGGER.info("poll {} records", messages.size());
    }
    for (Message message : messages) {
      //先入库
      boolean duplicated = false;
      if (consumerRepository != null) {
        duplicated = consumerRepository.insert(message);
      }
      if (duplicated) {
        LOGGER.info(LoggingMarker.getLoggingMarker(message, true), "duplicate message, do nothing");
      } else {
        queue.enqueue(message);
        LOGGER.info(LoggingMarker.getLoggingMarker(message, true), "poll and enqueue");
      }
    }
    //暂停和恢复，避免过多的消息造成内存溢出
    if (pause) {
      //队列中等待的消息降到一半才恢复
      if (checkResumeCondition()) {
        resume();
      }
    } else {
      if (checkPauseCondition()) {
        pause();
        return 0;
      }
    }
    return messages.size();
  }

  protected final boolean checkPauseCondition() {
    return queue.isFull();
  }

  protected final boolean checkResumeCondition() {
    return queue.isLowWaterMark();
  }
}
