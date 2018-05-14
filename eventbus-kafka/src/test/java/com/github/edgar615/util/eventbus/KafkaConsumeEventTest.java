package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaConsumeEventTest extends EventbusTest {

  private static Logger logger = LoggerFactory.getLogger(KafkaConsumeEventTest.class);

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
    EventConsumer consumer = new KafkaEventConsumer(options);
    AtomicInteger count = new AtomicInteger();
    consumer.consumer(null, null, e -> {
      logger.info("---| handle {}", e);
      count.incrementAndGet();
//      int r = Integer.parseInt(e.action().resource());
//      if (r % 5 == 0) {
//        try {
//          TimeUnit.SECONDS.sleep(3);
//        } catch (InterruptedException e1) {
//          e1.printStackTrace();
//        }
//      }
    });
    try {
      TimeUnit.SECONDS.sleep(30);
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
