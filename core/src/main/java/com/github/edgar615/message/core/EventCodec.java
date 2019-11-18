package com.github.edgar615.message.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class EventCodec implements MessageBodyCodec {
  @Override
  public MessageBody decode(Map<String, Object> map) {
    String resource = (String) map.get("resource");
    Map<String, Object> content = (Map<String, Object>) map.get("content");
    return Event.create(resource, content);
  }

  @Override
  public Map<String, Object> encode(MessageBody action) {
    Event event = (Event) action;
    Map<String, Object> map = new HashMap<>();
    map.put("resource", event.resource());
    map.put("content", event.content());
    return map;
  }

  @Override
  public String name() {
    return Event.TYPE;
  }
}
