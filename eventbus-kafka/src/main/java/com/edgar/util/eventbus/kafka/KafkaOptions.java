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
public class KafkaOptions {

  /**
   * The default number of event loop threads to be used  = 2 * number of cores on the machine
   */
  public static final int DEFAULT_EVENT_LOOP_POOL_SIZE =
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


  private static String DEFAULT_SERIALIZER_CLASS =
          "org.apache.kafka.common.serialization.StringSerializer";

  private static String DEFAULT_DESERIALIZER_CLASS =
          "org.apache.kafka.common.serialization.StringDeserializer";

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

  /**
   * key的序列化类
   */
  private String keySerializerClass = DEFAULT_SERIALIZER_CLASS;

  /**
   * value的序列化类
   */
  private String valueSerializerClass = DEFAULT_SERIALIZER_CLASS;

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
   * key的反序列化类
   */
  private String keyDeserializerClass = DEFAULT_DESERIALIZER_CLASS;

  /**
   * value的反序列化类
   */
  private String valueDeserializerClass = DEFAULT_DESERIALIZER_CLASS;

  /**
   * 订阅的主题
   */
  private List<String> consumerTopics;

  /**
   * 线程数量
   */
  private int workerPoolSize = DEFAULT_EVENT_LOOP_POOL_SIZE;

//  /**
//   * 消费事件之后从回调函数
//   */
//  private Handler<EventFuture<Void>> consumerListener;
//
//  /**
//   * 收到消息之后的回调函数
//   */
//  private Handler<KafkaMessage> messageListener;


  public KafkaOptions() {

  }

  public Properties producerProps() {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    producerProps.put(ProducerConfig.ACKS_CONFIG, producerAcks);
    producerProps.put(ProducerConfig.RETRIES_CONFIG, producerRetries);
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, producerLingerMs);
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, producerBufferMemory);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
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
                 keyDeserializerClass);
    consumerProps
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                 valueDeserializerClass);
    return consumerProps;
  }

  public String getConsumerAutoOffsetRest() {
    return consumerAutoOffsetReset;
  }

  /**
   * 设置auto.offset.reset.仅支持earliest,latest,none三种配置
   *
   * @param consumerAutoOffsetReset auto.offset.reset
   * @return KafkaOptions
   */
  public KafkaOptions setConsumerAutoOffsetRest(String consumerAutoOffsetReset) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setBlockedCheckerMs(int blockedCheckerMs) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setWorkerPoolSize(int workerPoolSize) {
    this.workerPoolSize = workerPoolSize;
    return this;
  }

  public String getKeySerializerClass() {
    return keySerializerClass;
  }

  /**
   * 设置键的序列化类.
   *
   * @param keySerializerClass 实现Serializer接口
   * @return KafkaOptions
   */
  public KafkaOptions setKeySerializerClass(String keySerializerClass) {
    this.keySerializerClass = keySerializerClass;
    return this;
  }

  public String getValueSerializerClass() {
    return valueSerializerClass;
  }

  /**
   * 设置值的序列化类
   *
   * @param valueSerializerClass 序列化类 实现Serializer接口
   * @return KafkaOptions
   */
  public KafkaOptions setValueSerializerClass(String valueSerializerClass) {
    this.valueSerializerClass = valueSerializerClass;
    return this;
  }

  public String getPartitionClass() {
    return partitionClass;
  }

  /**
   * 设置分区类partitioner.class
   *
   * @param partitionClass 实现Partitioner接口
   * @return KafkaOptions
   */
  public KafkaOptions setPartitionClass(String partitionClass) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setProducerAcks(String producerAcks) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setProducerBatchSize(int producerBatchSize) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setProducerBufferMemory(int producerBufferMemory) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setProducerRetries(int producerRetries) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setProducerLingerMs(int producerLingerMs) {
    this.producerLingerMs = producerLingerMs;
    return this;
  }

  public boolean isConsumerAutoCommit() {
    return consumerAutoCommit;
  }

//    public KafkaOptions setConsumerAutoCommit(boolean consumerAutoCommit) {
//        this.consumerAutoCommit = consumerAutoCommit;
//        return this;
//    }

  public int getConsumerAutoCommitIntervalMs() {
    return consumerAutoCommitIntervalMs;
  }

  public KafkaOptions setConsumerAutoCommitIntervalMs(int consumerAutoCommitIntervalMs) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setConsumerSessionTimeoutMs(int consumerSessionTimeoutMs) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setServers(String servers) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setGroup(String group) {
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
   * @return KafkaOptions
   */
  public KafkaOptions setId(String id) {
    this.id = id;
    return this;
  }

  public String getKeyDeserializerClass() {
    return keyDeserializerClass;
  }

  /**
   * 设置消息键的反序列化类.
   *
   * @param keyDeserializerClass 反序列化类，实现Deserializer接口
   * @return
   */
  public KafkaOptions setKeyDeserializerClass(String keyDeserializerClass) {
    this.keyDeserializerClass = keyDeserializerClass;
    return this;
  }

  public String getValueDeserializerClass() {
    return valueDeserializerClass;
  }

  /**
   * 设置消息值的反序列化类.
   *
   * @param valueDeserializerClass 反序列化类，实现Deserializer接口
   * @return KafkaOptions
   */
  public KafkaOptions setValueDeserializerClass(String valueDeserializerClass) {
    this.valueDeserializerClass = valueDeserializerClass;
    return this;
  }

  public List<String> getConsumerTopics() {
    return consumerTopics;
  }

  /**
   * 设置订阅的主题.
   *
   * @param consumerTopics 主题集合
   * @return KafkaOptions
   */
  public KafkaOptions setConsumerTopics(List<String> consumerTopics) {
    this.consumerTopics = consumerTopics;
    return this;
  }

}
