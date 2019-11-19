package com.github.edgar615.message.redis;

import com.github.edgar615.message.bus.MessageProducer;
import com.github.edgar615.message.bus.MessageWriteStream;
import com.github.edgar615.message.bus.ProducerOptions;
import com.github.edgar615.message.core.Event;
import com.github.edgar615.message.core.Message;
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

    MessageWriteStream writeStream = new RedisMessageWriteStream(redisClient);
    MessageProducer producer = MessageProducer.create(new ProducerOptions(), writeStream);
    writeStream.start();
    producer.start();
    for (int i = 0; i < 10; i++) {
      Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
      Message message = Message.create("DeviceControlEvent", event);
      producer.send(message);
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    producer.close();

  }

}
