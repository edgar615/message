package com.edgar.util.event;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class MessageCodec implements EventActionCodec {
  @Override
  public EventAction decode(Map<String, Object> map) {
    String resource = (String) map.get("resource");
    Map<String, Object> content = (Map<String, Object>) map.get("content");
    return Message.create(resource, content);
  }

  @Override
  public Map<String, Object> encode(EventAction action) {
    Message message = (Message) action;
    Map<String, Object> map = new HashMap<>();
    map.put("resource", message.resource());
    map.put("content", message.content());
    return map;
  }

  @Override
  public String name() {
    return Message.TYPE;
  }
}
