package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;
import java.util.concurrent.CompletableFuture;

public interface EventBusWriteStream {

  CompletableFuture<Event> send(Event event);

  void close();
}
