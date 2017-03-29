package com.edgar.util.event;

/**
 * 消息活动，用于区分不同的消息类型.
 *
 * @author Edgar  Date 2017/3/8
 */
public interface EventAction {

  /**
   * Unknown action.
   */
  String UNKNOWN = "unknown";

  /**
   *
   * @return action名称
   */
  String name();
}
