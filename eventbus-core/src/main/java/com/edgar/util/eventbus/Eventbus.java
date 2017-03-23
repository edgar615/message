package com.edgar.util.eventbus;

import com.edgar.util.eventbus.event.Event;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public interface Eventbus {
  void send(Event event);
}
