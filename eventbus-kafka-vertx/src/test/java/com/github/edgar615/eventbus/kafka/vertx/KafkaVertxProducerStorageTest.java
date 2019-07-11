package com.github.edgar615.eventbus.kafka.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.event.Message;
import com.github.edgar615.eventbus.kafka.KafkaProducerOptions;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.Vertx;

import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2018/5/17.
 *
 * @author Edgar  Date 2018/5/17
 */
public class KafkaVertxProducerStorageTest {

  public static void main(String[] args) throws InterruptedException {
    Vertx vertx = Vertx.vertx();
    KafkaProducerOptions options = new KafkaProducerOptions()
            .setFetchPendingPeriod(100);
    options.setServers("120.76.158.7:9092");
    MockProducerStorage storage = new MockProducerStorage();
    KafkaVertxEventbusProducer producer = KafkaVertxEventbusProducer.create(vertx, options, storage);
    for (int i = 0; i < 10; i++) {
      Message message = Message.create("" + i, ImmutableMap.of("foo", "bar"));
      Event event = Event.create("DeviceControlEvent_1_3", message, 1);
      producer.send(event, ar ->{
        System.out.println(ar.result());
      });
//      TimeUnit.SECONDS.sleep(1);
    }
    try {
      TimeUnit.SECONDS.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    producer.close();

    System.out.println(producer.metrics());
    System.out.println(storage.events().size());
  }

}
