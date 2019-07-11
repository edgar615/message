package com.github.edgar615.eventbus.utils;

import static net.logstash.logback.marker.Markers.append;

import com.github.edgar615.eventbus.event.Event;
import org.slf4j.Marker;

public class LoggingMarker {

  public static Marker getLoggingMarker(Event event, boolean wasReceived) {
    Marker messageMarker =
        append("traceId", event.head().id())
            .and(append("topic", event.head().to()))
            .and(append("type", wasReceived ? "inbound" : "outbound"))
            .and(append("action", event.head()))
            .and(append("source", event.toMap()));
    return messageMarker;
  }

  public static Marker getIdLoggingMarker(Event event) {
    Marker messageMarker =
        append("traceId", event.head().id());
    return messageMarker;
  }
}
