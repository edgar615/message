package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.metrics.Metrics;
import com.google.common.base.Strings;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Producer的配置属性.
 *
 * @author Edgar  Date 2016/5/17
 */
public class KafkaProducerOptions extends ProducerOptions {

  public static final int DEFAULT_LINGER_MS = 1;

  private static final int DEFAULT_BATCH_SIZE = 16384;

  private static final int DEFAULT_BUFFER_MEMORY = 33554432;

  private static final int DEFAULT_RETRIES = 0;

  private static final String DEFAULT_ACKS = "all";

  private static String DEFAULT_PARTITION_CLASS = null;

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

  public KafkaProducerOptions() {

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
                      "com.github.edgar615.util.eventbus.EventSerializer");
    if (!Strings.isNullOrEmpty(partitionClass)) {
      producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionClass);
    }
    return producerProps;
  }

  @Override
  public KafkaProducerOptions setFetchPendingPeriod(long fetchPendingPeriod) {
    super.setFetchPendingPeriod(fetchPendingPeriod);
    return this;
  }

  public String getPartitionClass() {
    return partitionClass;
  }

  /**
   * 设置分区类partitioner.class
   *
   * @param partitionClass 实现Partitioner接口
   * @return KafkaProducerOptions
   */
  public KafkaProducerOptions setPartitionClass(String partitionClass) {
    this.partitionClass = partitionClass;
    return this;
  }

  /**
   * 设置限流的最大配额，当未处理的事件超过配额时，需要拒绝发送
   *
   * @param maxQuota
   * @return KafkaProducerOptions
   */
  public KafkaProducerOptions setMaxQuota(long maxQuota) {
    super.setMaxQuota(maxQuota);
    return this;
  }

  public String getAcks() {
    return acks;
  }

  /**
   * 设置 acks.
   *
   * @param acks acks
   * @return KafkaProducerOptions
   */
  public KafkaProducerOptions setAcks(String acks) {
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
   * @return KafkaProducerOptions
   */
  public KafkaProducerOptions setBatchSize(int batchSize) {
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
   * @return KafkaProducerOptions
   */
  public KafkaProducerOptions setBufferMemory(int bufferMemory) {
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
   * @return KafkaProducerOptions
   */
  public KafkaProducerOptions setRetries(int retries) {
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
   * @return KafkaProducerOptions
   */
  public KafkaProducerOptions setLingerMs(int lingerMs) {
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
   * @return KafkaProducerOptions
   */
  public KafkaProducerOptions setServers(String servers) {
    this.servers = servers;
    return this;
  }

}
