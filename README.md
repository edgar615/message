# eventbus
消息适配

# 消息格式
消息格式为JSON格式，所有字符编码均为UTF-8格式。消息格式分为消息头和消息体。
<table>
<tr>
<th>字段</th>
<th>内容</th>
</tr>
<tr>
<td>head</td>
<td>JSON对象，表示消息头部各种属性</td>
</tr>
<tr>
<td>data</td>
<td>JSON对象，表示消息内容，通常为用户自定义消息内容</td>
</tr>
</table>


## 消息头
消息头字段定义了消息的ID，发送信道，接收信道，组信道，消息活动等各种信息。消息头字段是消息的关键内容。

<table>
<tr>
<th>字段</th>
<th>内容</th>
</tr>
<td>to</td>
<td>消息接收者信道</td>
</tr>
<tr>
<td>action</td>
<td>消息活动，用于区分不同的消息类型</td>
</tr>
<tr>
<td>id</td>
<td>消息id，唯一</td>
</tr>
<tr>
<td>timestamp</td>
<td>消息产生时间戳</td>
</tr>
<tr>
<td>${xxx}</td>
<td>扩展字段</td>
</tr>
</table>

${xxx}用来表示扩展字段，可以用来扩展消息头中未定义的属性，比如

<table>
<tr>
<th>字段</th>
<th>内容</th>
</tr>
<tr>
<td>from</td>
<td>消息发送者私有信道</td>
</tr>
<tr>
<td>group</td>
<td>消息发送者组信道</td>
</tr>
</table>

### 消息活动
消息活动目前仅实现了Message类型

- 消息：message

## 消息内容
消息内容为JSON格式文本，如果消息中存在二进制数据需要进行base64编码为文本串。消息体的编码格式为UTF-8，对于不同的action，消息内容格式不同。

### 消息 message
单向消息，不需要接收方（或者是消息订阅方）回应

<table>
<tr>
<th>字段</th>
<th>内容</th>
</tr>
<tr>
<td>resource</td>
<td>资源标识（字符串）</td>
</tr>
<tr>
<td>content</td>
<td>请求参数（JSON对象）</td>
</tr>
</table>

# 发送消息
## 示例

    KafkaProducerOptions options = new KafkaProducerOptions();
    options.setServers("10.11.0.31:9092");
    EventProducer producer = new KafkaEventProducer(options);
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message, 1);
      producer.send(event);
    }

EventProducer内部使用了一个队列保存所有要发送的消息，按照入队顺序将消息发送到消息服务器

## 发送消息的事务
对于一些对数据一致性要求不高，对系统稳定性，业务逻辑没有影响的消息（例如：短信通知，邮件通知），我们并不关心消息是否发送成功、是否丢失。
但有一些对数据一致性要求的高的业务，我们需要保证消息成功投递到消息服务器。

以一个使用微服务实现的网上商城为例，该商城包括三个服务

1. 客户服务维护包括客户的各种信息，例如积分
2. 订单服务管理订单
3. 库存服务管理库存

当客户创建订单时，需要通知库存服务预留库存、客户服务增加积分，一般操作如下：

1. 创建一个新订单 NewOrder，订单表插入一行数据
2. 发送一个创建订单的事件 OrderCreatedEvent.
3. 客户服务收到OrderCreatedEvent，更新积分
4. 库存服务收到OrderCreatedEvent，出库，发送出库事件

考虑到数据的最终一致性，数据库更新（步骤1）和发布事件（步骤2）这两个操作必须是一个原子操作。
如果它们不是原子操作，当步骤1更新数据库后，步骤2的OrderCreatedEvent事件发布之前，订单服务挂了，系统就变成不一致状态。
因此发布事件应用采用multi-step process involving only local transactions的策略：

在业务实体库中创建一个EVENT表，用于存储消息列表。业务服务发起一个（本地）数据库交易，更新业务实体状态，向EVENT表中插入一个事件，然后提交此次交易。
另外一个独立应用进程或者线程（定时任务）查询此EVENT表，向消息代理发布事件，然后使用本地交易标志此事件为已发布.

例如订单服务向订单表插入一行，然后向EVENT表中插入OrderCreatedEvent，后台一个定时任务查询EVENT表中未发布的事件并发布他们，在发布成功后更新EVENT表标志此事件为已发布。

通过向ProducerOptions中注册ProducerStorage实现类可以实现上述功能

    options.setProducerStorage(storage);

该接口有四个方法需要实现

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

注册ProducerStorage之后，EventProducer内部会启用一个定时任务，定时调用ProducerStorage的pendingList方法，从持久层查询需要发送的消息。
再消息发送成功或者失败之后，会对持久层中的消息做状态标记：status 1-成功，2-失败 3-过期

ProducerStorage里所有的方法应该尽可能的编写非阻塞的代码，避免影响消息发送的效率。


# 消费消息

    String server = "10.11.0.31:9092";
    KafkaConsumerOptions options = new KafkaConsumerOptions();
    options.setServers(server)
        .setGroup("test-consumer")
        .addTopic("test");
    EventConsumer consumer = new KafkaEventConsumer(options);
    consumer.consumer(null, null, e -> {
      logger.info("---| handle {}", e);
     });

内部会使用一个固定数量的线程池来并发的消费消息

## 消息的串行处理

虽然同一个消息队列中的消息是顺序读取的，但是出于性能的考虑，我们一般是采用并发的方式消费消息。

但是保证同一用户（设备、分类）的消息按照顺序处理是应用的常见需求。

通过向ConsumerOptions中注册Partitioner实现类可以实现上述功能

    options.setPartitioner(partitioner);

该接口只有一个方法需要实现

    int partition(Event event);

Consumer在从消息服务器读取到消息之后会根据数据值将分片后的消息推送到一个私有队列，线程池按照顺序依次执行队列中的分片消息。

    options.setServers(server)
      .setPartitioner(event -> {
        Message message = (Message) event.action();
        int userId = (int) message.content().get("userId");
        return userId % 5;
      })

## 消息处理
通过 consumer(BiPredicate<String, String> predicate, EventHandler handler)  方法可以为对应的消息绑定处理函数

BiPredicate通过比较消息头中的to和消息体的resource来判断hanler能否处理该消息

    eventConsumer.consumer(
            (t, r) -> t.startsWith("DeviceControlEvent") || t.startsWith("DeviceChangeEvent"),
            handler);

上述例子就表示handler只处理两种消息：

  to 以 DeviceControlEvent 开头
  to 以 DeviceChangeEvent 开头

consumer(String topic, String resource, EventHandler handler)方法会在内部通过
consumer(BiPredicate<String, String> predicate, EventHandler handler) 来绑定处理函数。

eventConsumer.consumer(null, "ProductVersionAddEvent", handler) 表示resource=ProductVersionAddEvent的消息

eventConsumer.consumer("DeviceChangeEvent", null, handler) 表示to=DeviceChangeEvent的消息

eventConsumer.consumer(null, null, handler) 表示所有的消息

**注意：**
consumer方法并未对handler的绑定做去重处理，如果同时绑定了下面两个handler

  eventConsumer.consumer("DeviceChangeEvent", null, handler1)
  eventConsumer.consumer(null, null, handler2) 表示所有的消息

那么to=DeviceChangeEvent的消息，会同时被handler1和handler2执行

# 关闭钩子
有时候停止应用的时候，应用内部的队列可能还有未处理完成的任务，所以需要一个平滑关闭的过程。
示例：
```
   Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        producer.close();
        //等待任务处理完成
        long start = System.currentTimeMillis();
        while (producer.waitForSend() > 0) {
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });
```

```
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        consumer.close();
        //等待任务处理完成
        long start = System.currentTimeMillis();
        while (consumer.waitForHandle() > 0) {
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });
```