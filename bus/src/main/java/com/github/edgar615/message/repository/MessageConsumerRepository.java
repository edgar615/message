package com.github.edgar615.message.repository;

import com.github.edgar615.message.core.Message;
import java.util.List;

/**
 * 消息消费的持久层，用来保证消息的幂等，已经避免消息丢失
 */
public interface MessageConsumerRepository {

  /**
   * 插入一个事件，插入的过程中需要判断任务是否是重复消息，重复消息不在继续处理
   * @return 重复消息false，非重复消息 true
   */
  boolean insert(Message message);

  /**
   * 从数据库中取出十条未处理的事件
   *
   * @return 待发送的事件列表
   */
  default List<Message> waitingForConsume() {
    return waitingForConsume(10);
  }

  /**
   * 从数据库中取出未处理的事件
   *
   * @param fetchCount 最大数量
   * @return 事件列表
   */
  List<Message> waitingForConsume(int fetchCount);

  /**
   * 标记事件
   *
   * @param eventId 事件ID
   * @param state 1-待处理，2-处理成功 3-处理失败 4-过期
   */
  void mark(String eventId, ConsumeMessageState state);

}
