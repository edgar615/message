package com.edgar.util.eventbus;

import com.google.common.base.Strings;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Producer的配置属性.
 *
 * @author Edgar  Date 2016/5/17
 */
public class ProducerOptions {

  private static long DEFAULT_PERIOD = 5 * 60 * 1000;

  private static int DEFAULT_SEND_MAXSIZE = 10000;

  public static final int DEFAULT_LINGER_MS = 1;

  private static final int DEFAULT_BATCH_SIZE = 16384;

  private static final int DEFAULT_BUFFER_MEMORY = 33554432;

  private static final int DEFAULT_RETRIES = 0;

  private static final String DEFAULT_ACKS = "all";

  private static String DEFAULT_PARTITION_CLASS = null;

  /**
   * 从存储层查询待发送事件的间隔，单位毫秒
   */
  private long fetchPendingPeriod = DEFAULT_PERIOD;

  private SendStorage sendStorage;

  private int maxSendSize = DEFAULT_SEND_MAXSIZE;

  //producer
  private String servers;

  /**
   * 响应,可选值 0, 1, all
   */
  private String acks = DEFAULT_ACKS;

  /**
   * 批量提交的大小
   */
  private int batchSize = DEFAULT_BATCH_SIZE;

  /**
   * 批量提交的大小
   */
  private int bufferMemory = DEFAULT_BUFFER_MEMORY;

  /**
   * 重试的次数
   */
  private int retries = DEFAULT_RETRIES;

  private int lingerMs = DEFAULT_LINGER_MS;

  /**
   * 分区类
   */
  private String partitionClass = DEFAULT_PARTITION_CLASS;

  public ProducerOptions() {

  }

  public Properties toProps() {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    producerProps.put(ProducerConfig.ACKS_CONFIG, acks);
    producerProps.put(ProducerConfig.RETRIES_CONFIG, retries);
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                      "com.edgar.util.eventbus.EventSerializer");
    if (!Strings.isNullOrEmpty(partitionClass)) {
      producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionClass);
    }
    return producerProps;
  }

  public long getFetchPendingPeriod() {
    return fetchPendingPeriod;
  }

  public ProducerOptions setFetchPendingPeriod(long fetchPendingPeriod) {
    this.fetchPendingPeriod = fetchPendingPeriod;
    return this;
  }

  public int getMaxSendSize() {
    return maxSendSize;
  }

  public ProducerOptions setMaxSendSize(int maxSendSize) {
    this.maxSendSize = maxSendSize;
    return this;
  }

  public SendStorage getSendStorage() {
    return sendStorage;
  }

  public ProducerOptions setSendStorage(SendStorage sendStorage) {
    this.sendStorage = sendStorage;
    return this;
  }

  public String getPartitionClass() {
    return partitionClass;
  }

  /**
   * 设置分区类partitioner.class
   *
   * @param partitionClass 实现Partitioner接口
   * @return ProducerOptions
   */
  public ProducerOptions setPartitionClass(String partitionClass) {
    this.partitionClass = partitionClass;
    return this;
  }

  public String getAcks() {
    return acks;
  }

  /**
   * 设置 acks.
   *
   * @param acks acks
   * @return ProducerOptions
   */
  public ProducerOptions setAcks(String acks) {
    this.acks = acks;
    return this;
  }

  public int getBatchSize() {
    return batchSize;
  }

  /**
   * 设置batch.size.
   *
   * @param batchSize batch.size
   * @return ProducerOptions
   */
  public ProducerOptions setBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public int getBufferMemory() {
    return bufferMemory;
  }

  /**
   * 设置buffer.memory .
   *
   * @param bufferMemory buffer.memory
   * @return ProducerOptions
   */
  public ProducerOptions setBufferMemory(int bufferMemory) {
    this.bufferMemory = bufferMemory;
    return this;
  }

  public int getRetries() {
    return retries;
  }

  /**
   * 设置retries.
   *
   * @param retries retries
   * @return ProducerOptions
   */
  public ProducerOptions setRetries(int retries) {
    this.retries = retries;
    return this;
  }

  public int getLingerMs() {
    return lingerMs;
  }

  /**
   * 设置linger.ms.
   *
   * @param lingerMs
   * @return ProducerOptions
   */
  public ProducerOptions setLingerMs(int lingerMs) {
    this.lingerMs = lingerMs;
    return this;
  }

  public String getServers() {
    return servers;
  }

  /**
   * 设置kafka的地址.
   *
   * @param servers kafka的地址
   * @return ProducerOptions
   */
  public ProducerOptions setServers(String servers) {
    this.servers = servers;
    return this;
  }

}
