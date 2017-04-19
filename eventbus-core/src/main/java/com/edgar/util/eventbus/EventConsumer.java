package com.edgar.util.eventbus;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
public interface EventConsumer {
  void consumer(String topic, String resource, EventHandler handler);
}
