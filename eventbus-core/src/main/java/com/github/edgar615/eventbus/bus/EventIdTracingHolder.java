package com.github.edgar615.eventbus.bus;

/**
 * Created by Edgar on 2018/7/14.
 *
 * @author Edgar  Date 2018/7/14
 */
public class EventIdTracingHolder {
  private static final ThreadLocal<EventIdTracing> CLIENT_HOLDER = new ThreadLocal<>();

  private EventIdTracingHolder() {
    throw new AssertionError("Not instantiable: " + EventIdTracingHolder.class);
  }

  public static void set(EventIdTracing principal) {
    CLIENT_HOLDER.set(principal);
  }

  public static EventIdTracing get() {
    return CLIENT_HOLDER.get();
  }

  public static void clear() {
    CLIENT_HOLDER.remove();
  }
}
