package com.edgar.util.eventbus;

import com.edgar.util.event.Event;

/**
 * 消息发送的接口.
 *
 * @author Edgar  Date 2017/3/24
 */
public interface SendBackend {

  EventFuture<Void> send(Event event);

  static SendBackend create(ProducerOptions options) {
    return new SendBackendImpl(options);
  }
}
