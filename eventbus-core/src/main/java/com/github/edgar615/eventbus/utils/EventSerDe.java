package com.github.edgar615.eventbus.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.edgar615.eventbus.event.Event;
import java.util.Map;

public class EventSerDe {

  public static String serialize(Event event) {
    try {
      if (event == null) {
        return null;
      } else {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = event.toMap();
        return mapper.writeValueAsString(map);
      }
    } catch (Exception e) {
      throw new SerDeException(
          "Error when serializing Event to String: " + e.getMessage());
    }
  }


  public static Event deserialize(String topic, String data) {
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
      throw new SerDeException(
          "Error when deserializing String to Event: " + e.getMessage());
    }
  }
}
