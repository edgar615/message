package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.function.Function;

/**
 * Created by Edgar on 2018/5/12.
 *
 * @author Edgar  Date 2018/5/12
 */
public class MockConsumer extends EventConsumerImpl {
  MockConsumer(ConsumerOptions options) {
    super(options);
  }

  MockConsumer(ConsumerOptions options,
               ConsumerStorage consumerStorage,
               Function<Event, String> identificationExtractor,
               Function<Event, Boolean> blackListFilter) {
    super(options, consumerStorage, identificationExtractor, blackListFilter);
  }

  public void pollEvent(Event event) {
    enqueue(event);
  }

  @Override
  public void pause() {

  }

  @Override
  public void resume() {

  }
}
