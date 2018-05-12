package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.Map;

/**
 * <b>发布事件</b>.
 * <p>
 * 下面的测试结果仅做参考，并未严格的进行性能测试。
 * 在本地I5机器上测试，kafka为单节点。分别用1、4、5、8个线程发送3W、10W、25W、50W、100W的数据（不主动调用producer.flush），分别用时
 * <pre>
 * 1个线程：1秒，3秒，7秒，13秒，20秒
 * 4个线程：1.5秒，3秒，6秒，12秒，23秒
 * 5个线程：1秒，3秒，6秒，12秒，23秒
 * 8个线程：1.5秒，4秒，6秒，12秒，23秒
 * </pre>
 * 跟进上面的数据，多线程对发送事件的性能并没有直接提升，因此我们采用单线程的方式对待发送的事件排队发送
 * <p>
 * <p>
 * 发送消息的持久化
 * 以一个用微服务实现的电子商城为例：
 * 1. 客户服务维护包括客户的各种信息，例如积分
 * 2. 订单服务管理订单
 * 3. 库存服务管理库存
 * <p>
 * 当客户创建订单时，需要通知库存服务预留库存、客户服务增加积分
 * 1. 创建一个新订单 NewOrder，订单表插入一行数据
 * 2. 发送一个创建订单的事件 OrderCreatedEvent.
 * 3. 客户服务收到OrderCreatedEvent，更新积分
 * 4. 库存服务收到OrderCreatedEvent，出库，发送出库事件
 * <p>
 * 考虑到数据的最终一致性，数据库更新（步骤1）和发布事件（步骤2）这两个操作必须是一个原子操作。
 * 如果更新数据库后，OrderCreatedEvent事件发布之前，订单服务挂了，系统就变成不一致状态。
 * 为了确保原子操作的标准方式是使用一个分布式事务(2PC或3PC)，但这样并不能从根本解决问题（CAP理论）
 * 为了保证这两个操作的原子性，我们有两种方式处理：
 * 1.发布事件应用采用multi-step process involving only local
 * transactions：在业务实体库中创建一个EVENT表，用于存储消息列表。业务服务发起一个（本地）数据库交易，更新业务实体状态，向EVENT
 * 表中插入一个事件，然后提交此次交易。另外一个独立应用进程或者线程（定时任务）查询此EVENT表，向消息代理发布事件，然后使用本地交易标志此事件为已发布.
 * 例如订单服务向订单表插入一行，然后向EVENT表中插入OrderCreatedEvent，后台一个定时任务查询EVENT表中未发布的事件并发布他们，在发布成功后更新EVENT
 * 表标志此事件为已发布
 * 2.挖掘数据库的交易日志（如MySQL的binlog）将日志发布给消息代理
 * <p>
 * <p>
 * ${@link EventProducer}采用第一个方式，如果${@link EventProducer}注册了持久化接口${@link ProducerStorage}，会先通过${@link ProducerStorage}将事件持久化，然后才会将事件加入待发送队列
 *
 * @author Edgar  Date 2017/4/19
 */
public interface EventProducer {

  /**
   * 发送消息.
   * 这个方法会将消息放入一个内部队列，然后按顺序发送消息.
   *
   * @param event
   */
  void send(Event event);

  /**
   * 关闭
   */
  void close();

  /**
   * 返回度量指标
   *
   * @return
   */
  Map<String, Object> metrics();

  /**
   * 等待被发送的消息
   *
   * @return
   */
  long waitForSend();
}
