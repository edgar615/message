package com.edgar.util.event;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * 请求消息表示消息发送端向接收端发起一个功能请求，请求消息的action为request..
 *
 * @author Edgar  Date 2016/4/18
 */
class RequestImpl implements Request {
  private final Map<String, Object> content;

  private final String operation;

  private final String resource;

  RequestImpl(String resource, String operation, Map<String, Object> content) {
    Preconditions.checkNotNull(resource, "resource cannot be null");
    this.content = content;
    this.resource = resource;
    this.operation = operation;
  }

  @Override
  public Map<String, Object> content() {
    return content;
  }

  @Override
  public String operation() {
    return operation;
  }

  @Override
  public String resource() {
    return resource;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Request")
            .add("resource", resource)
            .add("operation", operation)
            .add("content", content)
            .toString();
  }

  @Override
  public String name() {
    return TYPE;
  }

}
