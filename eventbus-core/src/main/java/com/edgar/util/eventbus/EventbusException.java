package com.edgar.util.eventbus;

/**
 * Created by Edgar on 2017/3/24.
 *
 * @author Edgar  Date 2017/3/24
 */
public class EventbusException extends RuntimeException {

  public EventbusException() {
    super();
  }

  public EventbusException(String message) {
    super(message);
  }

  public EventbusException(String message, Throwable cause) {
    super(message, cause);
  }

}
