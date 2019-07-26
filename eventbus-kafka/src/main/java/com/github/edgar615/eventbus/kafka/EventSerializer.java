package com.github.edgar615.eventbus.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.edgar615.eventbus.event.Event;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * 为了避免有人写入错误的格式，这里不通过kafka做序列化，直接在程序做序列化
 */
@Deprecated
public class EventSerializer implements Serializer<Event> {
  private String encoding = "UTF8";
  private ObjectMapper mapper = new ObjectMapper();
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
    Object encodingValue = configs.get(propertyName);
    if (encodingValue == null) { encodingValue = configs.get("serializer.encoding"); }
    if (encodingValue != null && encodingValue instanceof String) {
      encoding = (String) encodingValue;
    }
  }

  @Override
  public byte[] serialize(String topic, Event data) {
    try {
      if (data == null) {
        return null;
      } else {
        Map<String, Object> map = data.toMap();
        return mapper.writeValueAsString(map).getBytes();
      }
    } catch (Exception e) {
      throw new SerializationException(
              "Error when serializing Event to byte[]: " + e.getMessage());
    }
  }

  @Override
  public void close() {
    // nothing to do
  }
}
