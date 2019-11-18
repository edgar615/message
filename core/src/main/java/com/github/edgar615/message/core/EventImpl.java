package com.github.edgar615.message.core;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * 即时消息为标准消息，发送消息内容，不要求应答。发送方不关心处理结果.
 *
 * @author Edgar  Date 2016/4/18
 */
class EventImpl implements Event {

  /**
   * 消息内容
   */
  private final Map<String, Object> content;

  /**
   * 资源标识
   */
  private final String resource;

  EventImpl(String resource, Map<String, Object> content) {
    Preconditions.checkNotNull(resource, "resource can not be null");
    Preconditions.checkNotNull(content, "content can not be null");
    this.resource = resource;
    this.content = ImmutableMap.copyOf(content);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Event")
            .add("content", content)
            .add("resource", resource)
            .toString();
  }

  @Override
  public Map<String, Object> content() {
    return content;
  }

  @Override
  public String resource() {
    return resource;
  }

  @Override
  public String name() {
    return TYPE;
  }

}
