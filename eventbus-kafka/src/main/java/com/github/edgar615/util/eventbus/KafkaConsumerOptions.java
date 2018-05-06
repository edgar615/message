package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.metrics.Metrics;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import com.github.edgar615.util.event.Event;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/**
 * Created by Edgar on 2016/5/17.
 *
 * @author Edgar  Date 2016/5/17
 */
public class KafkaConsumerOptions extends ConsumerOptions {

  private static final boolean DEFAULT_AUTO_COMMIT = true;

  private static final int DEFAULT_SESSION_TIMEOUT_MS = 30000;

  private static final String DEFAULT_SERVERS = "localhost:9092";

  private static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";

  private static final int DEFAULT_MAX_POLL_RECORDS = 500;

  /**
   * 订阅的主题
   */
  private final Set<String> topics = new HashSet<>();

  /**
   * 是否自动提交，默认值false
   */
  private boolean consumerAutoCommit = DEFAULT_AUTO_COMMIT;

  /**
   * 一个consumer只能定义一个pattern。只要指定了topics，pattern就不起作用
   */
  private String pattern;

  /**
   * 自动提交的间隔时间，默认值1000
   */
//  private int consumerAutoCommitIntervalMs = DEFAULT_AUTO_COMMIT_INTERVAL_MS;

  /**
   * 消费者session的过期时间，默认值30000
   */
  private int consumerSessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;

  private String consumerAutoOffsetReset = DEFAULT_AUTO_OFFSET_RESET;

  private int maxPollRecords = DEFAULT_MAX_POLL_RECORDS;

  private String servers = DEFAULT_SERVERS;

  private String group;

  private Map<TopicPartition, Long> startingOffset = new HashMap<>();

  public KafkaConsumerOptions() {

  }

  public Properties consumerProps() {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumerAutoCommit);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAutoOffsetReset);
    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumerSessionTimeoutMs);
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    consumerProps
            .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                 "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                 "com.github.edgar615.util.eventbus.EventDeserializer");
    return consumerProps;
  }

  public Long getStartingOffset(TopicPartition tp) {
    return startingOffset.getOrDefault(tp, -2l);
  }

  public Map<TopicPartition, Long> getStartingOffset() {
    return ImmutableMap.copyOf(startingOffset);
  }

  /**
   * 设置偏移量.
   * <p>
   * 慎用该方法，如果事件消费被阻塞，很容易引起消费者的重新分配，导致再次重置偏移量.
   *
   * @param tp
   * @param startingOffset
   * @return
   */
  public KafkaConsumerOptions addStartingOffset(TopicPartition tp, Long startingOffset) {
    this.startingOffset.put(tp, startingOffset);
    return this;
  }

  public String getConsumerAutoOffsetRest() {
    return consumerAutoOffsetReset;
  }

  /**
   * 设置auto.offset.reset.仅支持earliest,latest,none三种配置
   *
   * @param consumerAutoOffsetReset auto.offset.reset
   * @return KafkaConsumerOptions
   */
  public KafkaConsumerOptions setConsumerAutoOffsetRest(String consumerAutoOffsetReset) {
    Preconditions.checkArgument("earliest".equals(consumerAutoOffsetReset)
                                || "none".equals(consumerAutoOffsetReset)
                                || "latest".equals(consumerAutoOffsetReset),
                                "only support earliest | latest | none");
    this.consumerAutoOffsetReset = consumerAutoOffsetReset;
    return this;
  }

  public int getMaxPollRecords() {
    return maxPollRecords;
  }

  /**
   * 设置一次poll的最大数量
   *
   * @param maxPollRecords 最大数量
   * @return KafkaConsumerOptions
   */
  public KafkaConsumerOptions setMaxPollRecords(int maxPollRecords) {
    if (maxPollRecords > 0) {
      this.maxPollRecords = maxPollRecords;
    }
    return this;
  }

  /**
   * 设置blockedCheckerMs，如果超过blockedCheckerMs仍然未被处理完的事件会打印警告日志.
   *
   * @param blockedCheckerMs 最大阻塞时间
   * @return KafkaConsumerOptions
   */
  @Override
  public KafkaConsumerOptions setBlockedCheckerMs(int blockedCheckerMs) {
    super.setBlockedCheckerMs(blockedCheckerMs);
    return this;
  }

  /**
   * 设置worker线程池的大小，该线程池主要用户处理事件的业务逻辑
   *
   * @param workerPoolSize 线程池大小.
   * @return KafkaConsumerOptions
   */
  @Override
  public KafkaConsumerOptions setWorkerPoolSize(int workerPoolSize) {
    super.setWorkerPoolSize(workerPoolSize);
    return this;
  }

  public boolean isConsumerAutoCommit() {
    return consumerAutoCommit;
  }

  public KafkaConsumerOptions setConsumerAutoCommit(boolean consumerAutoCommit) {
    this.consumerAutoCommit = consumerAutoCommit;
    return this;
  }

  public int getConsumerSessionTimeoutMs() {
    return consumerSessionTimeoutMs;
  }

  /**
   * 设置consumer的超时时间.
   *
   * @param consumerSessionTimeoutMs 毫秒数
   * @return KafkaConsumerOptions
   */
  public KafkaConsumerOptions setConsumerSessionTimeoutMs(int consumerSessionTimeoutMs) {
    this.consumerSessionTimeoutMs = consumerSessionTimeoutMs;
    return this;
  }

  public List<String> getTopics() {
    return new ArrayList<>(topics);
  }

  /**
   * 设置订阅的主题.
   *
   * @param topic 主题
   * @return KafkaConsumerOptions
   */
  public KafkaConsumerOptions addTopic(String topic) {
    this.topics.add(topic);
    return this;
  }

  public String getPattern() {
    return pattern;
  }

  public KafkaConsumerOptions setPattern(String pattern) {
    this.pattern = pattern;
    return this;
  }

  public String getGroup() {
    return group;
  }

  public KafkaConsumerOptions setGroup(String group) {
    this.group = group;
    return this;
  }

  public String getServers() {
    return servers;
  }

  public KafkaConsumerOptions setServers(String servers) {
    this.servers = servers;
    return this;
  }

  /**
   * 设置限流的最大配额，当未处理的事件超过<code>maxQuota*quotaFactor</code>的时，需要暂停消费
   *
   * @param maxQuota
   * @return
   */
  @Override
  public KafkaConsumerOptions setMaxQuota(int maxQuota) {
    super.setMaxQuota(maxQuota);
    return this;
  }

  /**
   * 设置黑名单的过滤器，如果filter返回true，表示不处理这个事件
   *
   * @param filter
   * @return
   */
  @Override
  public KafkaConsumerOptions setBlackListFilter(
          Function<Event, Boolean> filter) {
    super.setBlackListFilter(filter);
    return this;
  }

  /**
   * 根据某个标识符，将事件按顺序处理
   *
   * @param identificationExtractor
   * @return KafkaConsumerOptions
   */
  @Override
  public KafkaConsumerOptions setIdentificationExtractor(
          Function<Event, String> identificationExtractor) {
    super.setIdentificationExtractor(identificationExtractor);
    return this;
  }

}
