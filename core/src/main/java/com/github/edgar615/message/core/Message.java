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
   * 创建一个Message对象
   *
   * @param header   消息头
   * @param body 消息活动
   * @return Message对象
   */
  static Message create(MessageHeader header, MessageBody body) {
    return new MessageImpl(header, body);
  }

  /**
   * 创建一个Message对象
   *
   * @param to     消息接收者信道
   * @param body 消息活动
   * @return Message对象
   */
  static Message create(String to, MessageBody body) {
    Preconditions.checkNotNull(body, "body cannot be null");
    MessageHeader head = MessageHeader.create(to, body.name());
    return create(head, body);
  }

  /**
   * 创建一个Message对象
   *
   * @param to     消息接收者信道
   * @param body 消息活动
   * @return Message对象
   */
  static Message create(String to, MessageBody body, long duration) {
    Preconditions.checkNotNull(body, "body cannot be null");
    MessageHeader head = MessageHeader.create(to, body.name(), duration);
    return create(head, body);
  }

  /**
   * 创建一个Message对象
   *
   * @param id     消息ID，消息ID请使用唯一的ID
   * @param to     消息接收者信道
   * @param body 消息活动
   * @return Message对象
   */
  static Message create(String id, String to, MessageBody body) {
    Preconditions.checkNotNull(body, "body cannot be null");
    MessageHeader head = MessageHeader.create(id, to, body.name());
    return create(head, body);
  }

  static Message fromMap(Map<String, Object> map) {
    Preconditions.checkArgument(map.containsKey("header"), "map must contains header");
    Preconditions.checkArgument(map.containsKey("body"), "map must contains body");
    Map<String, Object> headerMap = Maps.newHashMap((Map<String, Object>) map.get("header"));
    Preconditions.checkArgument(headerMap.containsKey("id"), "header must contains id");
    Preconditions.checkArgument(headerMap.containsKey("to"), "header must contains to");
    Preconditions.checkArgument(headerMap.containsKey("action"), "header must contains action");
    Preconditions.checkArgument(headerMap.containsKey("timestamp"), "header must contains timestamp");
    String id = (String) headerMap.get("id");
    String to = (String) headerMap.get("to");
    String action = (String) headerMap.get("action");
    long timestamp = Long.parseLong(headerMap.get("timestamp").toString());
    headerMap.remove("id");
    headerMap.remove("to");
    headerMap.remove("action");
    headerMap.remove("timestamp");

    MessageHeader head = new MessageHeaderImpl(id, to, action, timestamp);
    headerMap.forEach((k, v) -> {
      if (v != null) {
        head.addExt(k, v.toString());
      }
    });

    Map<String, Object> bodyMap = Maps.newHashMap((Map<String, Object>) map.get("body"));

    List<MessageBody> actions = codecList.stream()
        .filter(c -> action.equalsIgnoreCase(c.name()))
        .map(c -> c.decode(bodyMap))
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
    headerMap.put("action", header().action());
    headerMap.put("timestamp", header().timestamp());
    headerMap.putAll(header().ext());

    List<Map<String, Object>> actions = codecList.stream()
        .filter(c -> body().name().equalsIgnoreCase(c.name()))
        .map(c -> c.encode(body()))
        .collect(Collectors.toList());

    map.put("body", actions.get(0));
    return map;
  }
}
