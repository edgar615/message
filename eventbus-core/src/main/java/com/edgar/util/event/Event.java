package com.edgar.util.event;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/8.
 *
 * @author Edgar  Date 2017/3/8
 */
public interface Event {

  List<EventActionCodec> codecList
      = Lists.newArrayList(ServiceLoader.load(EventActionCodec.class));

  /**
   * @return 消息头
   */
  EventHead head();

  /**
   * @return 消息活动
   */
  EventAction action();

  /**
   * 创建一个Event对象
   *
   * @param head   消息头
   * @param action 消息活动
   * @return Event对象
   */
  static Event create(EventHead head, EventAction action) {
    return new EventImpl(head, action);
  }

  /**
   * 创建一个Event对象
   *
   * @param to     消息接收者信道
   * @param action 消息活动
   * @return Event对象
   */
  static Event create(String to, EventAction action) {
    Preconditions.checkNotNull(action, "action cannot be null");
    EventHead head = EventHead.create(to, action.name());
    return create(head, action);
  }

  /**
   * 创建一个Event对象
   *
   * @param to     消息接收者信道
   * @param action 消息活动
   * @return Event对象
   */
  static Event create(String to, EventAction action, long duration) {
    Preconditions.checkNotNull(action, "action cannot be null");
    EventHead head = EventHead.create(to, action.name(), duration);
    return create(head, action);
  }

  /**
   * 创建一个Event对象
   *
   * @param id     消息ID，消息ID请使用唯一的ID
   * @param to     消息接收者信道
   * @param action 消息活动
   * @return Event对象
   */
  static Event create(String id, String to, EventAction action) {
    Preconditions.checkNotNull(action, "action cannot be null");
    EventHead head = EventHead.create(id, to, action.name());
    return create(head, action);
  }

  /**
   * 创建一个Event对象
   *
   * @param id       消息ID，消息ID请使用唯一的ID
   * @param to       消息接收者信道
   * @param action   消息活动
   * @param duration 多长时间有效，单位秒，小于0为永不过期
   * @return
   */
  static Event create(String id, String to, EventAction action,
                      long duration) {
    Preconditions.checkNotNull(action, "action cannot be null");
    EventHead head = EventHead.create(id, to, action.name(), duration);
    return create(head, action);
  }

  static Event fromMap(Map<String, Object> map) {
    Preconditions.checkArgument(map.containsKey("head"), "map must contains head");
    Preconditions.checkArgument(map.containsKey("data"), "map must contains data");
    Map<String, Object> headerMap = Maps.newHashMap((Map<String, Object>) map.get("head"));
    Preconditions.checkArgument(headerMap.containsKey("id"), "head must contains id");
    Preconditions.checkArgument(headerMap.containsKey("to"), "head must contains to");
    Preconditions.checkArgument(headerMap.containsKey("action"), "head must contains action");
    Preconditions.checkArgument(headerMap.containsKey("timestamp"), "head must contains timestamp");
    String id = (String) headerMap.get("id");
    String to = (String) headerMap.get("to");
    String action = (String) headerMap.get("action");
    long timestamp = Long.parseLong(headerMap.get("timestamp").toString());
    long duration = -1;
    if (headerMap.containsKey("duration")) {
      duration = Long.parseLong(headerMap.get("duration").toString());
    }
    headerMap.remove("id");
    headerMap.remove("to");
    headerMap.remove("action");
    headerMap.remove("timestamp");
    headerMap.remove("duration");

    EventHead head = new EventHeadImpl(id, to, action, timestamp, duration);
    headerMap.forEach((k, v) -> {
      if (v != null) {
        head.addExt(k, v.toString());
      }
    });

    Map<String, Object> dataMap = Maps.newHashMap((Map<String, Object>) map.get("data"));

    List<EventAction> actions = codecList.stream()
        .filter(c -> action.equalsIgnoreCase(c.name()))
        .map(c -> c.decode(dataMap))
        .collect(Collectors.toList());
    EventAction eventAction = actions.get(0);

    return create(head, eventAction);
  }

  default Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    Map<String, Object> headerMap = new HashMap<>();
    map.put("head", headerMap);
    headerMap.put("id", head().id());
    headerMap.put("to", head().to());
    headerMap.put("action", head().action());
    headerMap.put("timestamp", head().timestamp());
    headerMap.put("duration", head().duration());
    headerMap.putAll(head().ext());

    List<Map<String, Object>> actions = codecList.stream()
        .filter(c -> action().name().equalsIgnoreCase(c.name()))
        .map(c -> c.encode(action()))
        .collect(Collectors.toList());

    map.put("data", actions.get(0));
    return map;
  }
}
