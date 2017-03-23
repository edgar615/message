package com.edgar.util.eventbus.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.edgar.util.eventbus.Eventbus;
import com.edgar.util.eventbus.event.Event;
import com.edgar.util.eventbus.event.Message;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class EventbusTest {

  public static void main(String[] args) {
    KafkaOptions kafkaOptions = new KafkaOptions()
            .setServers("localhost:9092")
            .setGroup("user")
            .setId("user-7889898")
            .setConsumerTopics(Lists.newArrayList("test_niot"));
    Eventbus eventbus = new EventbusImpl(kafkaOptions);

    for (int i = 0; i < 100; i ++) {
      Message message = Message.create("UserAdd", ImmutableMap.of("foo", "bar"));
      Event event = Event.create("test", message);
      eventbus.send(event);
    }

  }
}
