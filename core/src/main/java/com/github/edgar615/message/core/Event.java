package com.github.edgar615.message.core;

import java.util.Map;

/**
 * 单向消息，不需要接收方（或者是消息订阅方）回应.
 *
 * @author Edgar  Date 2017/3/8
 */
public interface Event extends MessageBody {

  String TYPE = "event";

  /**
   * @return 请求参数
   */
  Map<String, Object> content();

  /**
   * 创建一个单向消息
   *
   * @param resource 资源标识
   * @param content  请求参数
   * @return Event
   */
  static Event create(String resource, Map<String, Object> content) {
    return new EventImpl(resource, content);
  }
}
