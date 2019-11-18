package com.github.edgar615.message.core;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Created by Edgar on 2017/3/8.
 *
 * @author Edgar  Date 2017/3/8
 */
class MessageImpl implements Message {

  private final MessageHeader header;

  private final MessageBody body;

  MessageImpl(MessageHeader header, MessageBody body) {
    Preconditions.checkNotNull(header, "header can not be null");
    Preconditions.checkNotNull(body, "body can not be null");
    this.header = header;
    this.body = body;
  }

  @Override
  public MessageHeader header() {
    return header;
  }

  @Override
  public MessageBody body() {
    return body;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Message")
        .add("header", header)
        .add("body", body)
        .toString();
  }
}
