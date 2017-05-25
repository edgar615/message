package com.edgar.util.eventbus;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaConsumeEventTest extends EventbusTest {

  private static Logger logger = LoggerFactory.getLogger(KafkaConsumeEventTest.class);

  public static void main(String[] args) {

    String server = "test.ihorn.com.cn:9092";
    KafkaConsumerOptions options = new KafkaConsumerOptions();
    options.setServers(server)
            .setGroup("test-b")
            .addTopic("DeviceControlEvent_1_3")
            .setMaxQuota(500)
            .addStartingOffset(new TopicPartition("DeviceControlEvent_1_3", 0), 0l)
        .setConsumerAutoOffsetRest("earliest");
    EventConsumer consumer = new KafkaEventConsumer(options);
    consumer.consumer(null, null, e -> {
//      logger.info("---| handle {}", e);
//      if (e.action().resource().equals("5")) {
//        try {
//          TimeUnit.SECONDS.sleep(5);
//        } catch (InterruptedException e1) {
//          e1.printStackTrace();
//        }
//      }
    });
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
