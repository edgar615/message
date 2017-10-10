package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Message;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            .setBlackListFilter(e -> {
              Message message  = (Message) e.action();
              String id = (String) message.content().get("id");
              return "accf23ea2f39".equalsIgnoreCase(id);
            })
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
