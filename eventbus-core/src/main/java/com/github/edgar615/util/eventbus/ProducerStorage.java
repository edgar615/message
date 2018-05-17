package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.List;

/**
 * 需要发送的事件可以通过这个接口实现持久化.
 * <p>
 * 该接口的实现最好能够对需要持久化的事件进行过滤${@link #shouldStorage(Event)}，对能够允许丢失的事件，例如短信、邮件，可以忽略掉持久化.
 * <p>
 * 该接口还提供了一个方法从存储中读取待发送的事件{@link #pendingList()}，
 * ${@link EventProducer}会启用一个定时任务，调用这个方法获取需要发送的事件，加入发送队列中。
 * <p>
 * 在事件发布之后，会调用${@link #mark(Event, int)}来向存储层标记事件的发布结果，这个方法应该尽量不要阻塞线程，否则会影响发布事件的性能。
 * 存储层应该记录事件失败的次数，超过一定次数的事件可以不再通过{@link #pendingList()}方法查询.
 */
public interface ProducerStorage {

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
  void save(Event event);

  /**
   * @return 待发送的事件列表
   */
  List<Event> pendingList();

  /**
   * 标记事件,这个方法应该尽量不要阻塞线程，否则会影响发布事件的性能。
   *
   * @param event
   * @param status 1-成功，2-失败 3-过期
   */
  void mark(Event event, int status);

}
