package com.github.edgar615.util.eventbus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaConsumeEventStorageTest extends EventbusTest {

  private static Logger logger = LoggerFactory.getLogger(KafkaConsumeEventStorageTest.class);

  public static void main(String[] args) {

    String server = "120.76.158.7:9092";
    KafkaConsumerOptions options = new KafkaConsumerOptions();
    options.setServers(server)
            .setGroup("test-c")
//            .setPattern(".*")
            .addTopic("DeviceControlEvent_1_3")
            .setMaxPollRecords(1)
            .setMaxQuota(5)
            .setConsumerAutoOffsetRest("earliest");
    MockConsumerStorage storage = new MockConsumerStorage();
    EventConsumer consumer = new KafkaEventConsumer(options, storage, null, null);
    AtomicInteger count = new AtomicInteger();
    consumer.consumer(null, null, e -> {
      logger.info("---| handle {}", e);
      count.incrementAndGet();
    });
    try {
      TimeUnit.SECONDS.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    consumer.close();
    try {
      TimeUnit.SECONDS.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    System.out.println(consumer.metrics());
    System.out.println(consumer.metrics().get("eventbus.consumer.completed"));
    System.out.println(count);
    System.out.println(storage.getEvents().size());
//    eventbus.consumer("test", null, e -> {
//      System.out.println("handle" + e);
//    });
//
//    eventbus.consumer("test", "1", e -> {
//      System.out.println("handle" + e);
//    });

//    ExecutorService executorService = Executors.newFixedThreadPool(1);
//    executorService.submit(() -> {
//      while (true) {
//        TimeUnit.SECONDS.sleep(5);
//        System.out.println(eventbus.metrics());
//      }
//    });
  }


}
