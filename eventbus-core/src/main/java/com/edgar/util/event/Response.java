package com.edgar.util.event;

import java.util.Map;

/**
 * 请求回应为请求消息的回应.
 *
 * @author Edgar  Date 2017/3/8
 */
public interface Response extends EventAction {
  String TYPE = "response";

  /**
   *
   * @return 0 成功非0失败
   */
  int result();

  /**
   *
   * @return 对应的请求ID
   */
  String reply();

  /**
   *
   * @return 回应结果
   */
  Map<String, Object> content();

  static Response create(int result, String reply, Map<String, Object> content) {
    return new ResponseImpl(result, reply, content);
  }
}
