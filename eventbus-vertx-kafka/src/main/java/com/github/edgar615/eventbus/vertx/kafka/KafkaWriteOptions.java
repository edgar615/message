package com.github.edgar615.eventbus.vertx.kafka;

import com.github.edgar615.eventbus.bus.ProducerOptions;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Producer的配置属性.
 *
 * @author Edgar  Date 2016/5/17
 */
public class KafkaWriteOptions extends ProducerOptions {

  private final Map<String, String> configs = new HashMap<>();

  public KafkaWriteOptions(Map<String, String> configs) {
    Objects.requireNonNull(configs);
    this.configs.putAll(configs);
    this.configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    this.configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  }

  public Map<String, String> getConfigs() {
    return configs;
  }
}
