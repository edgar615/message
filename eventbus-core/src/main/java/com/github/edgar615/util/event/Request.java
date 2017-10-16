package com.github.edgar615.util.event;

import java.util.Map;

/**
 * 请求消息表示消息发送端向接收端发起一个功能请求，需要等待接受端响应.
 *
 * @author Edgar  Date 2017/3/8
 */
@Deprecated
public interface Request extends EventAction {

  String TYPE = "request";

  /**
   * @return 请求参数
   */
  Map<String, Object> content();

  /**
   * @return 操作类型
   */
  String operation();

  /**
   * 创建一个Request.
   *
   * @param resource 资源标识，接口
   * @param operation 操作类型，方法
   * @param content 请求参数
   * @return
   */
  static Request create(String resource, String operation,
                                             Map<String, Object> content) {
    return new RequestImpl(resource, operation, content);
  }
}
