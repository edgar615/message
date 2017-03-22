package com.edgar.util.eventbus.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class EventDeserializer implements Deserializer<Map> {
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
  public Map<String, Object> deserialize(String topic, byte[] data) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      if (data == null) {
        return null;
      } else {
        return mapper.readValue(data, Map.class);
      }
    } catch (Exception e) {
      throw new SerializationException(
              "Error when deserializing byte[] to Map: " + e.getMessage());
    }
  }

  @Override
  public void close() {
    // nothing to do
  }
}