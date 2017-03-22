package com.edgar.util.eventbus.kafka;

import com.google.common.collect.Lists;

import com.edgar.util.eventbus.Eventbus;

import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class EventbusTest {

  public static void main(String[] args) {
    KafkaOptions kafkaOptions = new KafkaOptions()
            .setServers("test.ihorn.com.cn:9092")
            .setGroup("user")
            .setId("user-7889898")
            .setConsumerTopics(Lists.newArrayList("test_niot"));
    Eventbus eventbus = new EventbusImpl(kafkaOptions);
  }
}
