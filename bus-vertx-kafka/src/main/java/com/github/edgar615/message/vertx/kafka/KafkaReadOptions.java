package com.github.edgar615.message.vertx.kafka;

import com.google.common.collect.ImmutableMap;
import io.vertx.kafka.client.common.TopicPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Created by Edgar on 2016/5/17.
 *
 * @author Edgar  Date 2016/5/17
 */
public class KafkaReadOptions {

  /**
   * 订阅的主题
   */
  private final Set<String> topics = new HashSet<>();

  /**
   * 一个consumer只能定义一个pattern。只要指定了topics，pattern就不起作用
   */
  private String pattern;

  private final Map<TopicPartition, Long> startingOffset = new HashMap<>();

  private final Map<String, String> configs = new HashMap<>();

  public KafkaReadOptions(Map<String, String> configs) {
    Objects.requireNonNull(configs);
    this.configs.putAll(configs);
    this.configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    this.configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
  }

  public Long getStartingOffset(TopicPartition tp) {
    return startingOffset.getOrDefault(tp, -2L);
  }

  public Map<TopicPartition, Long> getStartingOffset() {
    return ImmutableMap.copyOf(startingOffset);
  }

  /**
   * 设置偏移量.
   *
   * <pre>
   * -2 默认
   * 0 beginning
   * -1 end
   * 具体的数字 偏移量
   * </pre>
   * <p>
   * 慎用该方法，如果事件消费被阻塞，很容易引起消费者的重新分配，导致再次重置偏移量.
   */
  public KafkaReadOptions addStartingOffset(TopicPartition tp, Long startingOffset) {
    this.startingOffset.put(tp, startingOffset);
    return this;
  }


  public List<String> getTopics() {
    return new ArrayList<>(topics);
  }

  /**
   * 设置订阅的主题.
   *
   * @param topic 主题
   * @return KafkaReadOptions
   */
  public KafkaReadOptions addTopic(String topic) {
    this.topics.add(topic);
    return this;
  }

  public String getPattern() {
    return pattern;
  }

  public KafkaReadOptions setPattern(String pattern) {
    this.pattern = pattern;
    return this;
  }

  public Map<String, String> getConfigs() {
    return configs;
  }
}
