package com.github.edgar615.message.kafka;

import com.github.edgar615.message.bus.ConsumerOptions;
import com.github.edgar615.message.bus.MessageConsumer;
import com.github.edgar615.message.utils.DefaultMessageQueue;
import com.github.edgar615.message.utils.MessageQueue;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaConsumeEventTest {

  private static Logger logger = LoggerFactory.getLogger(KafkaConsumeEventTest.class);

  public static void main(String[] args) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.203:9092");
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, "user");
    KafkaReadOptions options = new KafkaReadOptions(configs)
        .addTopic("DeviceControlEvent");
    MessageQueue messageQueue = DefaultMessageQueue.create(100);
    KafkaMessageReadStream readStream = new KafkaMessageReadStream(messageQueue, null, options);
    ConsumerOptions consumerOptions = new ConsumerOptions()
        .setWorkerPoolSize(5)
        .setBlockedCheckerMs(1000);
    MessageConsumer consumer = MessageConsumer.create(consumerOptions, messageQueue);
    consumer.consumer(null, null, e -> {
      logger.info("---| handle {}", e);
    });
    consumer.start();
    readStream.start();
  }

}
