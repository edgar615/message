package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;
import java.util.List;

public interface EventBusReadStream {

  void pause();

  void resume();

  List<Event> poll();

}
