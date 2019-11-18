package com.github.edgar615.message.vertx;

import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.repository.SendMessageState;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.util.List;

/**
 * Created by Edgar on 2019/7/16.
 *
 * @author Edgar  Date 2019/7/16
 */
public interface VertxMessageProducerRepository {

  /**
   * 事件的持久化.
   *
   * @param message 事件
   */
  void insert(Message message, Handler<AsyncResult<Void>> resultHandler);

  /**
   * 从数据库中取出十条未处理的事件
   *
   * @return 待发送的事件列表
   */
  default void waitingForSend(Handler<AsyncResult<List<Message>>> resultHandler) {
    waitingForSend(10, resultHandler);
  }

  /**
   * 从数据库中取出未处理的事件
   *
   * @param fetchCount 最大数量
   * @return 待发送的事件列表
   */
  void waitingForSend(int fetchCount, Handler<AsyncResult<List<Message>>> resultHandler);

  /**
   * 标记事件,这个方法应该尽量不要阻塞发布线程，否则会影响发布事件的性能。
   *
   * @param eventId 事件ID
   * @param state 1-待发送，2-发送成功 3-发送失败 4-过期
   */
  void mark(String eventId, SendMessageState state, Handler<AsyncResult<Void>> resultHandler);

}
