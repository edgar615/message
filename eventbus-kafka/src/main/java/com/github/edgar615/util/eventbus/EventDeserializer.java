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