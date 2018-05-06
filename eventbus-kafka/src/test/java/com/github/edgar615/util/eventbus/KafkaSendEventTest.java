package com.github.edgar615.util.eventbus;

import com.google.common.collect.ImmutableMap;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.event.Message;

import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaSendEventTest extends EventbusTest {

  public static void main(String[] args) throws InterruptedException {
    KafkaProducerOptions options = new KafkaProducerOptions();
    options.setServers("120.76.158.7:9092");
    EventProducer producer = new KafkaEventProducer(options);
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("DeviceControlEvent_1_3", message, 1);
      producer.send(event);
//      TimeUnit.SECONDS.sleep(1);
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    producer.close();

    System.out.println(producer.metrics());
  }

}
