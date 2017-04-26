package com.edgar.util.eventbus;

import java.util.function.BiPredicate;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
public interface EventConsumer {
  void consumer(BiPredicate<String, String> predicate, EventHandler handler);

  void consumer(String topic, String resource, EventHandler handler);
}
