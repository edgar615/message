package com.github.edgar615.eventbus.vertx.kafka;

import com.github.edgar615.eventbus.bus.ConsumerOptions;
import com.github.edgar615.eventbus.bus.EventBusConsumer;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.DefaultEventQueue;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.vertx.VertxEventBusConsumer;
import com.github.edgar615.eventbus.vertx.VertxEventBusReadStream;
import com.github.edgar615.eventbus.vertx.VertxEventHandler;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class SeekToBeginningConsumeEventTest {

  private static Logger logger = LoggerFactory.getLogger(SeekToBeginningConsumeEventTest.class);

  public static void main(String[] args) {
    Vertx vertx  = Vertx.vertx();
    Map<String, String> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.203:9092");
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, "user");
    KafkaReadOptions options = new KafkaReadOptions(configs)
        .addTopic("DeviceControlEvent");
    EventQueue eventQueue = DefaultEventQueue.create(100);
    VertxEventBusReadStream readStream = new VertxKafkaEventBusReadStream(vertx, eventQueue, null, options);
    ConsumerOptions consumerOptions = new ConsumerOptions()
        .setWorkerPoolSize(5)
        .setBlockedCheckerMs(1000);
    options.addStartingOffset(new TopicPartition("DeviceControlEvent", 0), 0L);
    VertxEventBusConsumer consumer = VertxEventBusConsumer.create(vertx, consumerOptions, eventQueue);
    VertxEventHandler handler = new VertxEventHandler() {
      @Override
      public void handle(Event event, Handler<AsyncResult<Event>> resultHandler) {
        logger.info("---| handle {}", event);
        resultHandler.handle(Future.succeededFuture(event));
      }
    };
    consumer.consumer(null, null, handler);
    consumer.start();
    readStream.start();

  }


}
