package com.github.edgar615.eventbus.kafka.vertx;

import com.github.edgar615.eventbus.event.Event;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * Created by Edgar on 2018/5/15.
 *
 * @author Edgar  Date 2018/5/15
 */
public interface VertxProducerStorage {
  /**
   * 过滤不需要持久化的事件
   *
   * @param event 事件
   * @return true：持久化，false：不做持久化
   */
  boolean shouldStorage(Event event);

  /**
   * 事件的持久化.
   *
   * @param event 事件
   */
  void save(Event event, Handler<AsyncResult<Void>> resultHandler);

  /**
   * @return 待发送的事件列表
   */
  void pendingList(Handler<AsyncResult<List<Event>>> resultHandler);

  /**
   * 标记事件,这个方法应该尽量不要阻塞线程，否则会影响发布事件的性能。
   *
   * @param event
   * @param status 1-成功，2-失败 3-过期
   */
  void mark(Event event, int status, Handler<AsyncResult<Void>> resultHandler);

}
