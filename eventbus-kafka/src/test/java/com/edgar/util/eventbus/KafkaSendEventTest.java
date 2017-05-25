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

  public static void main(String[] args) {
    KafkaProducerOptions options = new KafkaProducerOptions();
    options.setServers("10.11.0.31:9092")
    .setMaxQuota(100);
    EventProducer producer = new KafkaEventProducer(options);
    for (int i = 0; i < 1000; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message, 1);
      producer.send(event);
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
