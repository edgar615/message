package com.github.edgar615.message.bus;

import org.junit.Test;

public class HandlerKeyTest {

  @Test
  public void testNull() {
    HandlerKey key1 = new HandlerKey(null, null);
    HandlerKey key2 = new HandlerKey(null, null);
  }
}
