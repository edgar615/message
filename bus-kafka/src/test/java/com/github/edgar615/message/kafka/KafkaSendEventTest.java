package com.github.edgar615.message.kafka;

import com.github.edgar615.message.bus.MessageProducer;
import com.github.edgar615.message.bus.ProducerOptions;
import com.github.edgar615.message.core.Event;
import com.github.edgar615.message.core.Message;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaSendEventTest {

  public static void main(String[] args) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.203:9092");
    KafkaWriteOptions options = new KafkaWriteOptions(configs);
    KafkaMessageWriteStream writeStream = new KafkaMessageWriteStream(options);
    MessageProducer producer = MessageProducer.create(new ProducerOptions(), writeStream);
    producer.start();
    for (int i = 0; i < 10; i++) {
      Event event = Event.create("" + i, ImmutableMap.of("foo", "bar"));
      Message message = Message.create("DeviceControlEvent", event, 1);
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
