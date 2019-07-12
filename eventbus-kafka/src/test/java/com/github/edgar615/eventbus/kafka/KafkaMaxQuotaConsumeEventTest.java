package com.github.edgar615.eventbus.kafka;

import com.github.edgar615.eventbus.bus.EventBusConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaMaxQuotaConsumeEventTest extends EventbusTest {

  private static Logger logger = LoggerFactory.getLogger(KafkaMaxQuotaConsumeEventTest.class);

  public static void main(String[] args) {

    String server = "120.76.158.7:9092";
    KafkaConsumerOptions options = new KafkaConsumerOptions();
    options.setServers(server)
            .setGroup("test-c")
            .setConsumerAutoCommit(false)
//            .setPattern(".*")
            .addTopic("DeviceControlEvent_1_3")
            .setMaxQuota(1);
    EventBusConsumer consumer = new KafkaEventBusConsumer(options);
    AtomicInteger count = new AtomicInteger();
    consumer.consumer(null, null, e -> {
      try {
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      logger.info("---| handle {}", count.incrementAndGet());
    });
  }


}
