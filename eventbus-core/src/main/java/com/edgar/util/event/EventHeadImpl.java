package com.edgar.util.event;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 消息头.
 *
 * @author Edgar  Date 2016/4/18
 */
class EventHeadImpl implements EventHead {
  /**
   * 消息接收者信道
   */
  private final String to;

  /**
   * 消息活动
   */
  private final String action;

  /**
   * 消息id
   */
  private final String id;

  /**
   * 消息产生时间戳
   */
  private final long timestamp;

  /**
   * 扩展头，用于增加额外的消息头
   */
  private final Map<String, String> ext = new HashMap<>();

  /**
   * 多长时间有效，单位秒，小于0为永不过期
   */
  private final long duration;

  EventHeadImpl(String id, String to, String action, long timestamp, long duration) {
    Preconditions.checkNotNull(id, "id can not be null");
    Preconditions.checkNotNull(to, "to can not be null");
    Preconditions.checkNotNull(action, "action cannot be null");
    Preconditions.checkNotNull(timestamp, "timestamp cannot be null");
    Preconditions.checkNotNull(duration, "duration cannot be null");
    this.id = id;
    this.to = to;
    this.action = action;
    this.timestamp = timestamp;
    this.duration = duration;
  }

  EventHeadImpl(String id, String to, String action, long duration) {
    this(id, to, action, Instant.now().getEpochSecond(), duration);
  }

  @Override
  public EventHead addExts(Map<String, String> exts) {
    Preconditions.checkNotNull(exts, "exts can not be null");
    this.ext.putAll(exts);
    return this;
  }

  @Override
  public EventHead addExt(String name, String value) {
    Preconditions.checkNotNull(name, "id can not be null");
    Preconditions.checkNotNull(value, "from can not be null");
    this.ext.put(name, value);
    return this;
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper helper
        = MoreObjects.toStringHelper("header")
        .add("to", to)
        .add("action", action)
        .add("id", id)
        .add("timestamp", timestamp)
        .add("duration", duration);
    ext.forEach((k, v) -> helper.add(k, v));
    return helper.toString();
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public String to() {
    return to;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public String action() {
    return action;
  }

  @Override
  public long duration() {
    return duration;
  }

  @Override
  public Map<String, String> ext() {
    return ImmutableMap.copyOf(ext);
  }

  @Override
  public String ext(String name) {
    return ext.get(name);
  }
}
