package com.edgar.util.eventbus;

import com.google.common.collect.ImmutableMap;

import com.edgar.util.event.Event;
import com.edgar.util.event.Message;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaSendEventTest extends EventbusTest {

  @Test
  public void testSend() {
    ProducerOptions options = new ProducerOptions();
    options.setServers(server);
    Eventbus eventbus = new EventbusImpl(options, null);
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message, 1);
      eventbus.send(event);
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
