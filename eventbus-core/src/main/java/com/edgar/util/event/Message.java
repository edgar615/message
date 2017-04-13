package com.edgar.util.event;

import java.util.Map;

/**
 * 单向消息，不需要接收方（或者是消息订阅方）回应.
 *
 * @author Edgar  Date 2017/3/8
 */
public interface Message extends EventAction {

  String TYPE = "message";

  /**
   * @return 请求参数
   */
  Map<String, Object> content();

  /**
   * 创建一个单向消息
   *
   * @param resource 资源标识
   * @param content  请求参数
   * @return Message
   */
  static Message create(String resource, Map<String, Object> content) {
    return new MessageImpl(resource, content);
  }
}
