package com.edgar.util.event;

import java.util.Map;
import java.util.UUID;

/**
 * 消息头字段定义了消息的ID，发送信道，接收信道，组信道，消息活动等各种信息。消息头字段是消息的关键内容.
 *
 * @author Edgar  Date 2017/3/8
 */
public interface EventHead {

  EventHead addExts(Map<String, String> exts);

  /**
   * 增加额外属性
   *
   * @param name  属性名
   * @param value 属性值
   * @return
   */
  EventHead addExt(String name, String value);

  /**
   * @return 消息ID
   */
  String id();

  /**
   * @return 消息接收者信道
   */
  String to();

  /**
   * @return 消息生成时间，单位秒
   */
  long timestamp();

  /**
   * @return 消息活动，用于区分不同的消息类型
   */
  String action();

  /**
   * @return 多长时间内有效，单位秒
   */
  long duration();

  /**
   * 额外属性
   *
   * @return 不可变map
   */
  Map<String, String> ext();

  /**
   * 根据名称查询额外属性
   *
   * @param name 属性名
   * @return 属性值
   */
  String ext(String name);

  /**
   * 创建EventHead对象，消息ID使用UUID自动生成
   *
   * @param to     消息接收者信道
   * @param action 消息活动
   * @return
   */
  static EventHead create(String to, String action) {
    return create(UUID.randomUUID().toString(), to, action, -1);
  }

  /**
   * 创建EventHead对象，消息ID使用UUID自动生成
   *
   * @param to       消息接收者信道
   * @param action   消息活动
   * @param duration 多长时间有效，单位秒，小于0为永不过期
   * @return
   */
  static EventHead create(String to, String action, long duration) {
    return create(UUID.randomUUID().toString(), to, action, duration);
  }

  /**
   * 创建EventHead对象
   *
   * @param id     消息ID，消息ID请使用唯一的ID
   * @param to     消息接收者信道
   * @param action 消息活动
   * @return
   */
  static EventHead create(String id, String to, String action) {
    return create(id, to, action, -1);
  }

  /**
   * 创建EventHead对象
   *
   * @param id       消息ID，消息ID请使用唯一的ID
   * @param to       消息接收者信道
   * @param action   消息活动
   * @param duration 多长时间有效，单位秒，小于0为永不过期
   * @return
   */
  static EventHead create(String id, String to, String action, long duration) {
    return new EventHeadImpl(id, to, action, duration);
  }
}
