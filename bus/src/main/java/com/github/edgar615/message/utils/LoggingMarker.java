package com.github.edgar615.message.utils;

import static net.logstash.logback.marker.Markers.append;
import static net.logstash.logback.marker.Markers.appendEntries;

import com.github.edgar615.message.core.Message;
import java.util.Map;
import org.slf4j.Marker;

public class LoggingMarker {

  public static Marker getLoggingMarker(Message message, boolean wasReceived) {
    Marker messageMarker =
        append("traceId", message.header().id())
            .and(append("topic", message.header().to()))
            .and(append("type", wasReceived ? "inbound" : "outbound"))
            .and(append("body", message.body().name()))
            .and(append("source", message.toMap()));
    return messageMarker;
  }

  public static Marker getLoggingMarker(Message message, boolean wasReceived, Map<String, Object> extra) {
    Marker messageMarker =
        append("traceId", message.header().id())
            .and(append("topic", message.header().to()))
            .and(append("type", wasReceived ? "inbound" : "outbound"))
            .and(append("body", message.body().name()))
            .and(append("source", message.toMap()))
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
