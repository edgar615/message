package com.github.edgar615.message.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.edgar615.message.core.Message;
import java.util.Map;

public class MessageSerDe {

  public static String serialize(Message message) {
    try {
      if (message == null) {
        return null;
      } else {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = message.toMap();
        return mapper.writeValueAsString(map);
      }
    } catch (Exception e) {
      throw new SerDeException(
          "Error when serializing Message to String: " + e.getMessage());
    }
  }


  public static Message deserialize(String topic, String data) {
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
      throw new SerDeException(
          "Error when deserializing String to Message: " + e.getMessage());
    }
  }
}
