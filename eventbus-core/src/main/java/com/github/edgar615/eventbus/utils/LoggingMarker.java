package com.github.edgar615.eventbus.utils;

import static net.logstash.logback.marker.Markers.append;
import static net.logstash.logback.marker.Markers.appendEntries;

import com.github.edgar615.eventbus.event.Event;
import java.util.Map;
import org.slf4j.Marker;

public class LoggingMarker {

  public static Marker getLoggingMarker(Event event, boolean wasReceived) {
    Marker messageMarker =
        append("traceId", event.head().id())
            .and(append("topic", event.head().to()))
            .and(append("type", wasReceived ? "inbound" : "outbound"))
            .and(append("action", event.action().name()))
            .and(append("source", event.toMap()));
    return messageMarker;
  }

  public static Marker getLoggingMarker(Event event, boolean wasReceived, Map<String, Object> extra) {
    Marker messageMarker =
        append("traceId", event.head().id())
            .and(append("topic", event.head().to()))
            .and(append("type", wasReceived ? "inbound" : "outbound"))
            .and(append("action", event.action().name()))
            .and(append("source", event.toMap()))
            .and(appendEntries(extra));
    return messageMarker;
  }

  public static Marker getIdLoggingMarker(String id) {
    Marker messageMarker =
        append("traceId", id);
    return messageMarker;
  }

  public static Marker getLoggingMarker(String id, Map<String, Object> extra) {
    Marker messageMarker =
        append("traceId", id)
        .and(appendEntries(extra));
    return messageMarker;
  }
}
