package com.github.edgar615.eventbus.vertx.kafka;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.github.edgar615.eventbus.vertx.VertxEventBusProducer;
import com.github.edgar615.eventbus.vertx.VertxEventBusWriteStream;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.Vertx;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Created by Edgar on 2018/5/17.
 *
 * @author Edgar  Date 2018/5/17
 */
public class VertxKafkaProducerTest {

  public static void main(String[] args) {
    Vertx vertx  = Vertx.vertx();
    Map<String, String> configs = new HashMap<>();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.203:9092");
    KafkaWriteOptions options = new KafkaWriteOptions(configs);
    VertxEventBusWriteStream writeStream = new VertxKafkaEventBusWriteStream(vertx, options);
    VertxEventBusProducer producer = VertxEventBusProducer.create(vertx, writeStream);
    producer.start();
    for (int i = 0; i < 130; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("DeviceControlEvent", message, 1);
      producer.send(event, ar -> {

      });
    }
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    producer.close();
  }

}
