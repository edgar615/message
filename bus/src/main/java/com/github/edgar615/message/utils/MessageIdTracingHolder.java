package com.github.edgar615.message.utils;

/**
 * Created by Edgar on 2018/7/14.
 *
 * @author Edgar  Date 2018/7/14
 */
public class MessageIdTracingHolder {
  private static final ThreadLocal<MessageIdTracing> CLIENT_HOLDER = new ThreadLocal<>();

  private MessageIdTracingHolder() {
    throw new AssertionError("Not instantiable: " + MessageIdTracingHolder.class);
  }

  public static void set(MessageIdTracing principal) {
    CLIENT_HOLDER.set(principal);
  }

  public static MessageIdTracing get() {
    return CLIENT_HOLDER.get();
  }

  public static void clear() {
    CLIENT_HOLDER.remove();
  }
}
