package com.github.edgar615.message.core;

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
public interface Message {

  List<MessageBodyCodec> codecList
      = Lists.newArrayList(ServiceLoader.load(MessageBodyCodec.class));

  /**
   * @return 消息头
   */
  MessageHeader header();

  /**
   * @return 消息活动
   */
  MessageBody body();

  /**
   * 创建一个Event对象
   *
   * @param head   消息头
   * @param action 消息活动
   * @return Event对象
   */
  static Message create(MessageHeader head, MessageBody action) {
    return new MessageImpl(head, action);
  }

  /**
   * 创建一个Event对象
   *
   * @param to     消息接收者信道
   * @param action 消息活动
   * @return Event对象
   */
  static Message create(String to, MessageBody action) {
    Preconditions.checkNotNull(action, "body cannot be null");
    MessageHeader head = MessageHeader.create(to, action.name());
    return create(head, action);
  }

  /**
   * 创建一个Event对象
   *
   * @param to     消息接收者信道
   * @param action 消息活动
   * @return Event对象
   */
  static Message create(String to, MessageBody action, long duration) {
    Preconditions.checkNotNull(action, "body cannot be null");
    MessageHeader head = MessageHeader.create(to, action.name(), duration);
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
  static Message create(String id, String to, MessageBody action) {
    Preconditions.checkNotNull(action, "body cannot be null");
    MessageHeader head = MessageHeader.create(id, to, action.name());
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
  static Message create(String id, String to, MessageBody action,
                      long duration) {
    Preconditions.checkNotNull(action, "body cannot be null");
    MessageHeader head = MessageHeader.create(id, to, action.name(), duration);
    return create(head, action);
  }

  static Message fromMap(Map<String, Object> map) {
    Preconditions.checkArgument(map.containsKey("header"), "map must contains header");
    Preconditions.checkArgument(map.containsKey("data"), "map must contains data");
    Map<String, Object> headerMap = Maps.newHashMap((Map<String, Object>) map.get("header"));
    Preconditions.checkArgument(headerMap.containsKey("id"), "header must contains id");
    Preconditions.checkArgument(headerMap.containsKey("to"), "header must contains to");
    Preconditions.checkArgument(headerMap.containsKey("body"), "header must contains body");
    Preconditions.checkArgument(headerMap.containsKey("timestamp"), "header must contains timestamp");
    String id = (String) headerMap.get("id");
    String to = (String) headerMap.get("to");
    String action = (String) headerMap.get("body");
    long timestamp = Long.parseLong(headerMap.get("timestamp").toString());
    long duration = -1;
    if (headerMap.containsKey("duration")) {
      duration = Long.parseLong(headerMap.get("duration").toString());
    }
    headerMap.remove("id");
    headerMap.remove("to");
    headerMap.remove("body");
    headerMap.remove("timestamp");
    headerMap.remove("duration");

    MessageHeader head = new MessageHeaderImpl(id, to, action, timestamp, duration);
    headerMap.forEach((k, v) -> {
      if (v != null) {
        head.addExt(k, v.toString());
      }
    });

    Map<String, Object> dataMap = Maps.newHashMap((Map<String, Object>) map.get("data"));

    List<MessageBody> actions = codecList.stream()
        .filter(c -> action.equalsIgnoreCase(c.name()))
        .map(c -> c.decode(dataMap))
        .collect(Collectors.toList());
    MessageBody messageBody = actions.get(0);

    return create(head, messageBody);
  }

  default Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    Map<String, Object> headerMap = new HashMap<>();
    map.put("header", headerMap);
    headerMap.put("id", header().id());
    headerMap.put("to", header().to());
    headerMap.put("body", header().action());
    headerMap.put("timestamp", header().timestamp());
    headerMap.put("duration", header().duration());
    headerMap.putAll(header().ext());

    List<Map<String, Object>> actions = codecList.stream()
        .filter(c -> body().name().equalsIgnoreCase(c.name()))
        .map(c -> c.encode(body()))
        .collect(Collectors.toList());

    map.put("data", actions.get(0));
    return map;
  }
}
