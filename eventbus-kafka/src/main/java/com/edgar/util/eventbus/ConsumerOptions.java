package com.edgar.util.eventbus;

import com.google.common.base.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Created by Edgar on 2016/5/17.
 *
 * @author Edgar  Date 2016/5/17
 */
public class ConsumerOptions {

  /**
   * The default number of consumer worker threads to be used  = 2 * number of cores on the machine
   */
  public static final int DEFAULT_WORKER_POOL_SIZE =
          2 * Runtime.getRuntime().availableProcessors();

  private static final boolean DEFAULT_AUTO_COMMIT = false;

  private static final int DEFAULT_AUTO_COMMIT_INTERVAL_MS = 1000;

  private static final int DEFAULT_SESSION_TIMEOUT_MS = 30000;

  private static final String DEFAULT_SERVERS = "localhost:9092";

  private static final String DEFAULT_AUTO_OFFSET_RESET = "latest";

  private static int DEFAULT_BLOCKER_CHECKER_MS = 1000;

  /**
   * 是否自动提交，默认值false
   */
  private final boolean consumerAutoCommit = DEFAULT_AUTO_COMMIT;

  /**
   * 订阅的主题
   */
  private final Set<String> topics = new HashSet<>();

  /**
   * 线程数量
   */
  private int workerPoolSize = DEFAULT_WORKER_POOL_SIZE;

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

  private String servers = DEFAULT_SERVERS;

  private String group;

  public ConsumerOptions() {

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
                 "com.edgar.util.eventbus.EventDeserializer");
    return consumerProps;
  }

  public String getConsumerAutoOffsetRest() {
    return consumerAutoOffsetReset;
  }

  /**
   * 设置auto.offset.reset.仅支持earliest,latest,none三种配置
   *
   * @param consumerAutoOffsetReset auto.offset.reset
   * @return ConsumerOptions
   */
  public ConsumerOptions setConsumerAutoOffsetRest(String consumerAutoOffsetReset) {
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
   * @return ConsumerOptions
   */
  public ConsumerOptions setBlockedCheckerMs(int blockedCheckerMs) {
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
   * @return EventbusOptions
   */
  public ConsumerOptions setWorkerPoolSize(int workerPoolSize) {
    this.workerPoolSize = workerPoolSize;
    return this;
  }

  public boolean isConsumerAutoCommit() {
    return consumerAutoCommit;
  }

//    public ConsumerOptions setConsumerAutoCommit(boolean consumerAutoCommit) {
//        this.consumerAutoCommit = consumerAutoCommit;
//        return this;
//    }

  public int getConsumerAutoCommitIntervalMs() {
    return consumerAutoCommitIntervalMs;
  }

  public ConsumerOptions setConsumerAutoCommitIntervalMs(int consumerAutoCommitIntervalMs) {
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
   * @return ConsumerOptions
   */
  public ConsumerOptions setConsumerSessionTimeoutMs(int consumerSessionTimeoutMs) {
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
   * @return ConsumerOptions
   */
  public ConsumerOptions addTopic(String topic) {
    this.topics.add(topic);
    return this;
  }

  public String getGroup() {
    return group;
  }

  public ConsumerOptions setGroup(String group) {
    this.group = group;
    return this;
  }

  public String getServers() {
    return servers;
  }

  public ConsumerOptions setServers(String servers) {
    this.servers = servers;
    return this;
  }
}
