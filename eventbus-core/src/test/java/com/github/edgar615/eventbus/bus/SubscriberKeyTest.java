package com.github.edgar615.eventbus.bus;

import org.junit.Test;

public class SubscriberKeyTest {

  @Test
  public void testNull() {
    SubscriberKey key1 = new SubscriberKey(null, null);
    SubscriberKey key2 = new SubscriberKey(null, null);
  }
}
