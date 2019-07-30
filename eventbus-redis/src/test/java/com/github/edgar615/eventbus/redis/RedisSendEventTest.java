package com.github.edgar615.eventbus.redis;

import com.github.edgar615.eventbus.bus.EventBusProducer;
import com.github.edgar615.eventbus.bus.EventBusWriteStream;
import com.github.edgar615.eventbus.bus.ProducerOptions;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.google.common.collect.ImmutableMap;
import io.lettuce.core.RedisClient;
import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class RedisSendEventTest {

  public static void main(String[] args) {
    RedisClient redisClient = RedisClient.create("redis://tabao@192.168.1.204:6379/0");

    EventBusWriteStream writeStream = new RedisEventBusWriteStream(redisClient);
    EventBusProducer producer = EventBusProducer.create(new ProducerOptions(), writeStream);
    writeStream.start();
    producer.start();
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("DeviceControlEvent", message, 1);
      producer.send(event);
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    producer.close();

  }

}
