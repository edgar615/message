package com.github.edgar615.message.utils;

/**
 * 序列化，反序列化的异常.
 *
 * @author Edgar.
 */
public class SerDeException extends RuntimeException {

  public SerDeException(String message) {
    super(message, (Throwable) null, false, false);
  }

}
