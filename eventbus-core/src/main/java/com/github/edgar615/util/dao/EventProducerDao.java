package com.github.edgar615.util.dao;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.eventbus.EventProducer;
import java.util.List;

/**
 * 需要发送的事件可以通过这个接口实现持久化.
 * <p>
 * 该接口的实现最好能够对需要持久化的事件进行过滤${@link #shouldStorage(Event)}，对能够允许丢失的事件，例如短信、邮件，可以忽略掉持久化.
 * <p>
 * 该接口还提供了一个方法从存储中读取待发送的事件{@link #pendingList()}，
 * ${@link EventProducer}会启用一个定时任务，调用这个方法获取需要发送的事件，加入发送队列中。
 * <p>
 * 在事件发布之后，会调用${@link #mark(String, int)}来向存储层标记事件的发布结果，这个方法应该尽量不要阻塞线程，否则会影响发布事件的性能。
 * 存储层应该记录事件失败的次数，超过一定次数的事件可以不再通过{@link #pendingList()}方法查询.
 */
public interface EventProducerDao {

  /**
   * 插入一个事件
   * @param event
   */
  void insert(Event event);

  /**
   * @return 待发送的事件列表
   */
  List<Event> pendingList();

  /**
   * 标记事件,这个方法应该尽量不要阻塞线程，否则会影响发布事件的性能。
   *
   * @param eventId 事件ID
   * @param status 1-待发送，2-发送成功 3-发送失败 4-过期
   */
  void mark(String eventId, int status);

}
