package com.edgar.util.eventbus.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by Edgar on 2016/5/17.
 *
 * @author Edgar  Date 2016/5/17
 */
public class KafkaEventbusOptions {

  /**
   * The default number of consumer worker threads to be used  = 2 * number of cores on the machine
   */
  public static final int DEFAULT_WORKER_POOL_SIZE =
          2 * Runtime.getRuntime().availableProcessors();

  public static final int DEFAULT_LINGER_MS = 1;

  private static final int DEFAULT_BATCH_SIZE = 16384;

  private static final int DEFAULT_BUFFER_MEMORY = 33554432;

  private static final int DEFAULT_RETRIES = 0;

  private static final String DEFAULT_ACKS = "all";

  private static final boolean DEFAULT_AUTO_COMMIT = false;

  private static final int DEFAULT_AUTO_COMMIT_INTERVAL_MS = 1000;

  private static final int DEFAULT_SESSION_TIMEOUT_MS = 30000;

  private static final String DEFAULT_SERVERS = "localhost:9092";

  private static final String DEFAULT_ID = UUID.randomUUID().toString();

  private static final String DEFAULT_GROUP = DEFAULT_ID;

  private static final String DEFAULT_AUTO_OFFSET_RESET = "latest";

  private static String DEFAULT_PARTITION_CLASS = null;

  private static int DEFAULT_BLOCKER_CHECKER_MS = 1000;

  /**
   * 是否自动提交，默认值false
   */
  private final boolean consumerAutoCommit = DEFAULT_AUTO_COMMIT;

  //producer
  private String servers = DEFAULT_SERVERS;

  /**
   * 响应,可选值 0, 1, all
   */
  private String producerAcks = DEFAULT_ACKS;

  /**
   * 批量提交的大小
   */
  private int producerBatchSize = DEFAULT_BATCH_SIZE;

  /**
   * 批量提交的大小
   */
  private int producerBufferMemory = DEFAULT_BUFFER_MEMORY;

  /**
   * 重试的次数
   */
  private int producerRetries = DEFAULT_RETRIES;

  private int producerLingerMs = DEFAULT_LINGER_MS;

  //consumer

  /**
   * 分区类
   */
  private String partitionClass = DEFAULT_PARTITION_CLASS;

  /**
   * 自动提交的间隔时间，默认值1000
   */
  private int consumerAutoCommitIntervalMs = DEFAULT_AUTO_COMMIT_INTERVAL_MS;

  /**
   * 消费者session的过期时间，默认值30000
   */
  private int consumerSessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;

  private String consumerAutoOffsetReset = DEFAULT_AUTO_OFFSET_RESET;

  private int blockedCheckerMs = DEFAULT_BLOCKER_CHECKER_MS;

  /**
   * 消费者组，默认值UUID
   */
  private String group = DEFAULT_GROUP;

  /**
   * 消费者ID，默认值UUID
   */
  private String id = DEFAULT_ID;

  /**
   * 订阅的主题
   */
  private List<String> consumerTopics;

  /**
   * 线程数量
   */
  private int workerPoolSize = DEFAULT_WORKER_POOL_SIZE;

//  /**
//   * 消费事件之后从回调函数
//   */
//  private Handler<EventFuture<Void>> consumerListener;
//
//  /**
//   * 收到消息之后的回调函数
//   */
//  private Handler<KafkaMessage> messageListener;


  public KafkaEventbusOptions() {

  }

  public Properties producerProps() {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    producerProps.put(ProducerConfig.ACKS_CONFIG, producerAcks);
    producerProps.put(ProducerConfig.RETRIES_CONFIG, producerRetries);
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, producerLingerMs);
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, producerBufferMemory);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                      "com.edgar.util.eventbus.kafka.EventSerializer");
    if (!Strings.isNullOrEmpty(partitionClass)) {
      producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionClass);
    }
    return producerProps;
  }

  public Properties consumerProps() {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumerAutoCommit);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAutoOffsetReset);
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, consumerAutoCommitIntervalMs);
    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumerSessionTimeoutMs);
    consumerProps
            .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                 "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                 "com.edgar.util.eventbus.kafka.EventDeserializer");
    return consumerProps;
  }

  public String getConsumerAutoOffsetRest() {
    return consumerAutoOffsetReset;
  }

  /**
   * 设置auto.offset.reset.仅支持earliest,latest,none三种配置
   *
   * @param consumerAutoOffsetReset auto.offset.reset
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setConsumerAutoOffsetRest(String consumerAutoOffsetReset) {
    Preconditions.checkArgument("earliest".equals(consumerAutoOffsetReset)
                                || "none".equals(consumerAutoOffsetReset)
                                || "latest".equals(consumerAutoOffsetReset),
                                "only support earliest | latest | none");
    this.consumerAutoOffsetReset = consumerAutoOffsetReset;
    return this;
  }

  public int getBlockedCheckerMs() {
    return blockedCheckerMs;
  }

  /**
   * 设置blockedCheckerMs，如果超过blockedCheckerMs仍然未被处理完的事件会打印警告日志.
   *
   * @param blockedCheckerMs 最大阻塞时间
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setBlockedCheckerMs(int blockedCheckerMs) {
    this.blockedCheckerMs = blockedCheckerMs;
    return this;
  }

  public int getWorkerPoolSize() {
    return workerPoolSize;
  }

  /**
   * 设置worker线程池的大小，该线程池主要用户处理事件的业务逻辑
   *
   * @param workerPoolSize 线程池大小.
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setWorkerPoolSize(int workerPoolSize) {
    this.workerPoolSize = workerPoolSize;
    return this;
  }

  public String getPartitionClass() {
    return partitionClass;
  }

  /**
   * 设置分区类partitioner.class
   *
   * @param partitionClass 实现Partitioner接口
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setPartitionClass(String partitionClass) {
    this.partitionClass = partitionClass;
    return this;
  }

  public String getProducerAcks() {
    return producerAcks;
  }

  /**
   * 设置 acks.
   *
   * @param producerAcks acks
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setProducerAcks(String producerAcks) {
    this.producerAcks = producerAcks;
    return this;
  }

  public int getProducerBatchSize() {
    return producerBatchSize;
  }

  /**
   * 设置batch.size.
   *
   * @param producerBatchSize batch.size
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setProducerBatchSize(int producerBatchSize) {
    this.producerBatchSize = producerBatchSize;
    return this;
  }

  public int getProducerBufferMemory() {
    return producerBufferMemory;
  }

  /**
   * 设置buffer.memory .
   *
   * @param producerBufferMemory buffer.memory
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setProducerBufferMemory(int producerBufferMemory) {
    this.producerBufferMemory = producerBufferMemory;
    return this;
  }

  public int getProducerRetries() {
    return producerRetries;
  }

  /**
   * 设置retries.
   *
   * @param producerRetries retries
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setProducerRetries(int producerRetries) {
    this.producerRetries = producerRetries;
    return this;
  }

  public int getProducerLingerMs() {
    return producerLingerMs;
  }

  /**
   * 设置linger.ms.
   *
   * @param producerLingerMs
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setProducerLingerMs(int producerLingerMs) {
    this.producerLingerMs = producerLingerMs;
    return this;
  }

  public boolean isConsumerAutoCommit() {
    return consumerAutoCommit;
  }

//    public KafkaEventbusOptions setConsumerAutoCommit(boolean consumerAutoCommit) {
//        this.consumerAutoCommit = consumerAutoCommit;
//        return this;
//    }

  public int getConsumerAutoCommitIntervalMs() {
    return consumerAutoCommitIntervalMs;
  }

  public KafkaEventbusOptions setConsumerAutoCommitIntervalMs(int consumerAutoCommitIntervalMs) {
    this.consumerAutoCommitIntervalMs = consumerAutoCommitIntervalMs;
    return this;
  }

  public int getConsumerSessionTimeoutMs() {
    return consumerSessionTimeoutMs;
  }

  /**
   * 设置consumer的超时时间.
   *
   * @param consumerSessionTimeoutMs 毫秒数
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setConsumerSessionTimeoutMs(int consumerSessionTimeoutMs) {
    this.consumerSessionTimeoutMs = consumerSessionTimeoutMs;
    return this;
  }

  public String getServers() {
    return servers;
  }

  /**
   * 设置kafka的地址.
   *
   * @param servers kafka的地址
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setServers(String servers) {
    this.servers = servers;
    return this;
  }

  public String getGroup() {
    return group;
  }

  /**
   * 设置eventbus所属的消费者组，同一个分区下，同一个组的两个消费者只有一个能够读取消息.
   *
   * @param group 组名
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setGroup(String group) {
    this.group = group;
    return this;
  }

  public String getId() {
    return id;
  }

  /**
   * eventbus的ID.
   *
   * @param id id
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setId(String id) {
    this.id = id;
    return this;
  }

  public List<String> getConsumerTopics() {
    return consumerTopics;
  }

  /**
   * 设置订阅的主题.
   *
   * @param consumerTopics 主题集合
   * @return KafkaEventbusOptions
   */
  public KafkaEventbusOptions setConsumerTopics(List<String> consumerTopics) {
    this.consumerTopics = consumerTopics;
    return this;
  }

}
