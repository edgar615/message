package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class EventDeserializer implements Deserializer<Event> {
  private String encoding = "UTF8";

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
    Object encodingValue = configs.get(propertyName);
    if (encodingValue == null) { encodingValue = configs.get("deserializer.encoding"); }
    if (encodingValue != null && encodingValue instanceof String) {
      encoding = (String) encodingValue;
    }
  }

  @Override
  public Event deserialize(String topic, byte[] data) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      if (data == null) {
        return null;
      } else {
        Map<String, Object> map = mapper.readValue(data, Map.class);
        //在event中追加__topic表示这个事件是从那个主题读取的
        Map<String, Object> head = (Map<String, Object>) map.get("head");
        head.put("__topic", topic);
        return Event.fromMap(map);
      }
    } catch (Exception e) {
      throw new SerializationException(
              "Error when deserializing byte[] to Event: " + e.getMessage());
    }
  }

  @Override
  public void close() {
    // nothing to do
  }
}