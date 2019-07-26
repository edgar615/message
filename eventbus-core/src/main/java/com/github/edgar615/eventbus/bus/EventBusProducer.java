package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.repository.EventProducerRepository;
import java.util.concurrent.CompletableFuture;

/**
 * <b>发布事件</b>.
 *
 * 发送消息的持久化 以一个用微服务实现的电子商城为例： 1. 客户服务维护包括客户的各种信息，例如积分 2. 订单服务管理订单 3. 库存服务管理库存
 * <p>
 * 当客户创建订单时，需要通知库存服务预留库存、客户服务增加积分 1. 创建一个新订单 NewOrder，订单表插入一行数据 2. 发送一个创建订单的事件 OrderCreatedEvent. 3.
 * 客户服务收到OrderCreatedEvent，更新积分 4. 库存服务收到OrderCreatedEvent，出库，发送出库事件
 * <p>
 * 考虑到数据的最终一致性，数据库更新（步骤1）和发布事件（步骤2）这两个操作必须是一个原子操作。 如果更新数据库后，OrderCreatedEvent事件发布之前，订单服务挂了，系统就变成不一致状态。
 * 为了确保原子操作的标准方式是使用一个分布式事务(2PC或3PC)，但这样并不能从根本解决问题（CAP理论） 为了保证这两个操作的原子性，我们有两种方式处理：
 * 1.发布事件应用采用multi-step process involving only local transactions：在业务实体库中创建一个EVENT表，用于存储消息列表。业务服务发起一个（本地）数据库交易，更新业务实体状态，向EVENT
 * 表中插入一个事件，然后提交此次交易。另外一个独立应用进程或者线程（定时任务）查询此EVENT表，向消息代理发布事件，然后使用本地交易标志此事件为已发布.
 * 例如订单服务向订单表插入一行，然后向EVENT表中插入OrderCreatedEvent，后台一个定时任务查询EVENT表中未发布的事件并发布他们，在发布成功后更新EVENT
 * 表标志此事件为已发布 2.挖掘数据库的交易日志（如MySQL的binlog）将日志发布给消息代理
 * <p>
 *
 * @author Edgar  Date 2017/4/19
 */
public interface EventBusProducer {

  static EventBusProducer create(ProducerOptions options, EventBusWriteStream writeStream) {
    return new EventBusProducerImpl(options, writeStream, null);
  }

  static EventBusProducer create(ProducerOptions options, EventBusWriteStream writeStream,
      EventProducerRepository eventProducerRepository) {
    return new EventBusProducerImpl(options, writeStream, eventProducerRepository);
  }

  /**
   * 直接调用MQ的API发送消息.
   *
   * @param event 事件
   * @return future
   */
  CompletableFuture<Event> send(Event event);

  /**
   * 先将消息持久化，稍后通过一个线程发送消息，保证消息不丢 这里没有返回值，为了降低eventbus的复杂度，这里没有订阅消息是否发送成功，
   * 如果想知道事件是否发送成功，可以在dao的mark方法上增加通知。
   *
   * @param event 事件
   */
  void save(Event event);

  /**
   * 启动
   */
  void start();

  /**
   * 关闭
   */
  void close();

  /**
   * 等待被发送的消息
   */
  long waitForSend();
}
