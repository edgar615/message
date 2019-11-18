package com.github.edgar615.message.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.edgar615.message.core.Message;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * 为了避免有人写入错误的格式，这里不通过kafka做序列化，直接在程序做序列化
 */
@Deprecated
public class MessageDeserializer implements Deserializer<Message> {
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
  public Message deserialize(String topic, byte[] data) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      if (data == null) {
        return null;
      } else {
        Map<String, Object> map = mapper.readValue(data, Map.class);
        //在event中追加__topic表示这个事件是从那个主题读取的
        Map<String, Object> head = (Map<String, Object>) map.get("header");
        head.put("__topic", topic);
        return Message.fromMap(map);
      }
    } catch (Exception e) {
      throw new SerializationException(
              "Error when deserializing byte[] to Message: " + e.getMessage());
    }
  }

  @Override
  public void close() {
    // nothing to do
  }
}
