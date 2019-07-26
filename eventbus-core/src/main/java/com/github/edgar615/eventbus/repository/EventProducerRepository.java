package com.github.edgar615.eventbus.repository;

import com.github.edgar615.eventbus.bus.EventBusProducer;
import com.github.edgar615.eventbus.event.Event;
import java.util.List;

/**
 * 需要发送的事件可以通过这个接口实现持久化，避免数据丢失
 * <p>
 * 对能够允许丢失的事件，例如短信、邮件，可以忽略掉持久化.
 * <p>
 * 该接口还提供了一个方法从存储中读取待发送的事件{@link #waitingForSend()}， ${@link EventBusProducer}会启用一个定时任务，调用这个方法获取需要发送的事件，加入发送队列中。
 * <p>
 * 在事件发布之后，会调用${@link #mark(String, SendEventState)}来向存储层标记事件的发布结果，这个方法应该尽量不要阻塞线程，否则会影响发布事件的性能。
 * 存储层应该记录事件失败的次数，超过一定次数的事件可以不再通过{@link #waitingForSend()}方法查询.
 */
public interface EventProducerRepository {

  /**
   * 插入一个事件
   */
  void insert(Event event);

  /**
   * 从数据库中取出十条未处理的事件
   *
   * @return 待发送的事件列表
   */
  default List<Event> waitingForSend() {
    return waitingForSend(10);
  }

  /**
   * 从数据库中取出未处理的事件
   *
   * @param fetchCount 最大数量
   * @return 待发送的事件列表
   */
  List<Event> waitingForSend(int fetchCount);

  /**
   * 标记事件,这个方法应该尽量不要阻塞发布线程，否则会影响发布事件的性能。
   *
   * @param eventId 事件ID
   * @param state 1-待发送，2-发送成功 3-发送失败 4-过期
   */
  void mark(String eventId, SendEventState state);

}
