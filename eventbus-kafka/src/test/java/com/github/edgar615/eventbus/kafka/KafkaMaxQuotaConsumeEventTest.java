package com.github.edgar615.eventbus.kafka;

import com.github.edgar615.eventbus.bus.ConsumerOptions;
import com.github.edgar615.eventbus.bus.EventBusConsumer;
import com.github.edgar615.eventbus.bus.EventBusConsumerImpl;
import com.github.edgar615.eventbus.utils.DefaultEventQueue;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.NamedThreadFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaMaxQuotaConsumeEventTest  {

  private static Logger logger = LoggerFactory.getLogger(KafkaMaxQuotaConsumeEventTest.class);

  public static void main(String[] args) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.212:9092");
    configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, "user");
    KafkaReadOptions options = new KafkaReadOptions(configs)
        .addTopic("DeviceControlEvent");
    EventQueue eventQueue = new DefaultEventQueue(5);
    options.addStartingOffset(new TopicPartition("DeviceControlEvent", 0), 0L);
    KafkaEventBusReadStream readStream = new KafkaEventBusReadStream(eventQueue, null, options);

    ConsumerOptions consumerOptions = new ConsumerOptions()
        .setWorkerPoolSize(5)
        .setBlockedCheckerMs(1000);
    EventBusConsumer consumer = new EventBusConsumerImpl(consumerOptions, eventQueue, null);
//    consumer.setPartitioner(event -> Integer.parseInt(event.action().resource()) % 3);
    consumer.consumer(null, null, e -> {
      logger.info("---| handle {}", e);
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
    });
    consumer.start();
    readStream.start();
  }

}
