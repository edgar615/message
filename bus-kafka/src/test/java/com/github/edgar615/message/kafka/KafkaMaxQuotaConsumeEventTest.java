package com.github.edgar615.message.kafka;

import com.github.edgar615.message.bus.ConsumerOptions;
import com.github.edgar615.message.bus.MessageConsumer;
import com.github.edgar615.message.utils.DefaultMessageQueue;
import com.github.edgar615.message.utils.MessageQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaMaxQuotaConsumeEventTest  {

  private static Logger logger = LoggerFactory.getLogger(KafkaMaxQuotaConsumeEventTest.class);

  public static void main(String[] args) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.203:9092");
    configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, "user");
    KafkaReadOptions options = new KafkaReadOptions(configs)
        .addTopic("DeviceControlEvent");
    MessageQueue messageQueue = DefaultMessageQueue.create(5);
    options.addStartingOffset(new TopicPartition("DeviceControlEvent", 0), 0L);
    KafkaMessageReadStream readStream = new KafkaMessageReadStream(messageQueue, null, options);

    ConsumerOptions consumerOptions = new ConsumerOptions()
        .setWorkerPoolSize(5)
        .setBlockedCheckerMs(1000);
    MessageConsumer consumer = MessageConsumer.create(consumerOptions, messageQueue);
//    consumer.setPartitioner(core -> Integer.parseInt(core.body().resource()) % 3);
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
