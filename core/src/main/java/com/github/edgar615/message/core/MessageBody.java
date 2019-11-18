package com.github.edgar615.message.core;

/**
 * 消息活动，用于区分不同的消息类型.
 *
 * @author Edgar  Date 2017/3/8
 */
public interface MessageBody {

  /**
   * Unknown body.
   */
  String UNKNOWN = "unknown";

  /**
   * @return action名称
   */
  String name();

  /**
   * @return 资源标识
   */
  String resource();
}
