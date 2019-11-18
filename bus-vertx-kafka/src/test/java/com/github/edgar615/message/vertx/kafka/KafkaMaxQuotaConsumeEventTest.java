package com.github.edgar615.message.vertx.kafka;

import com.github.edgar615.message.bus.ConsumerOptions;
import com.github.edgar615.message.core.Message;
import com.github.edgar615.message.utils.DefaultMessageQueue;
import com.github.edgar615.message.utils.MessageQueue;
import com.github.edgar615.message.vertx.VertxMessageConsumer;
import com.github.edgar615.message.vertx.VertxMessageReadStream;
import com.github.edgar615.message.vertx.VertxMessageHandler;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class KafkaMaxQuotaConsumeEventTest {

  private static Logger logger = LoggerFactory.getLogger(KafkaMaxQuotaConsumeEventTest.class);

  public static void main(String[] args) {
    Vertx vertx  = Vertx.vertx();
    Map<String, String> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.203:9092");
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, "user");
    KafkaReadOptions options = new KafkaReadOptions(configs)
        .addTopic("DeviceControlEvent");
    MessageQueue messageQueue = DefaultMessageQueue.create(5);
    VertxMessageReadStream readStream = new VertxKafkaMessageReadStream(vertx, messageQueue, null, options);
    ConsumerOptions consumerOptions = new ConsumerOptions()
        .setWorkerPoolSize(5)
        .setBlockedCheckerMs(1000);
    options.addStartingOffset(new TopicPartition("DeviceControlEvent", 0), 100L);
    VertxMessageConsumer consumer = VertxMessageConsumer.create(vertx, consumerOptions,
        messageQueue);
    AtomicInteger count = new AtomicInteger();
    VertxMessageHandler handler = new VertxMessageHandler() {
      @Override
      public void handle(Message event, Handler<AsyncResult<Message>> resultHandler) {
        logger.info("---| handle {}", event);
        int seq = count.incrementAndGet();
        if (seq <= 5 || (seq > 10 && seq < 16) || (seq > 100 && seq < 110)) {
          try {
            TimeUnit.MILLISECONDS.sleep(300);
          } catch (InterruptedException e1) {
            e1.printStackTrace();
          }
        }

        resultHandler.handle(Future.succeededFuture(event));
      }
    };
    consumer.consumer(null, null, handler);
    consumer.start();
    readStream.start();
  }

}
