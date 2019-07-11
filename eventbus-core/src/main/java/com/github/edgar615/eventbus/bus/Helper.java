package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.EventAction;
import com.github.edgar615.eventbus.event.EventHead;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Edgar on 2017/3/24.
 *
 * @author Edgar  Date 2017/3/24
 */
public class Helper {

  public static String toHeadString(Event event) {
    EventHead head = event.head();
    StringBuilder s = new StringBuilder();
    s.append("id:").append(head.id()).append(";")
            .append("to:").append(head.to()).append(";")
            .append("action:").append(head.action()).append(";")
            .append("timestamp:").append(head.timestamp()).append(";")
            .append("duration:").append(head.duration()).append(";");
    head.ext().forEach((k, v) -> s.append(k).append(":").append(v).append(";"));

    return s.toString();
  }

  public static String toActionString(Event event) {
    EventAction action = event.action();
    List<String> actions = Event.codecList.stream()
            .filter(c -> action.name().equalsIgnoreCase(c.name()))
            .map(c -> c.encode(action))
            .map(m -> {
              StringBuilder s = new StringBuilder();
              m.forEach((k, v) -> s.append(k + ":" + v + ";"));
              return s.toString();
            })
            .collect(Collectors.toList());
    return actions.get(0);
  }

}
