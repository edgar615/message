package com.edgar.util.event;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * 请求应答为请求消息的应答，每个请求消息必须有应答消息，应答消息为请求操作的结果.
 *
 * @author Edgar  Date 2016/4/18
 */
class ResponseImpl implements Response {
  /**
   * 0 成功非0失败
   */
  private final int result;

  /**
   * 对应的请求ID，表示是针对哪条消息的回应.
   */
  private final String reply;

  /**
   * 回应结果
   */
  private final Map<String, Object> content;

  ResponseImpl(int result, String reply, Map<String, Object> content) {
    Preconditions.checkNotNull(reply, "operation cannot be null");
    Preconditions.checkNotNull(content, "content cannot be null");
    this.result = result;
    this.reply = reply;
    this.content = ImmutableMap.copyOf(content);
  }

  @Override
  public int result() {
    return result;
  }

  @Override
  public String reply() {
    return reply;
  }

  @Override
  public Map<String, Object> content() {
    return content;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Response")
            .add("result", result)
            .add("reply", reply)
            .add("content", content)
            .toString();
  }

  @Override
  public String name() {
    return TYPE;
  }

}