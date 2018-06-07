package com.github.edgar615.util.eventbus.vertx;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.eventbus.KafkaConsumerOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class VertxKafkaConsumeEventTest {

  private static Logger logger = LoggerFactory.getLogger(VertxKafkaConsumeEventTest.class);

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();
    String server = "120.76.158.7:9092";
    KafkaConsumerOptions options = new KafkaConsumerOptions();
    options.setServers(server)
            .setGroup("test-exre")
//            .setPattern(".*")
            .addTopic("DeviceControlEvent_1_3")
            .setMaxPollRecords(1)
            .setMaxQuota(5)
            .setConsumerAutoOffsetRest("earliest");
    KafkaVertxEventbusConsumer consumer = new KafkaVertxEventbusConsumerImpl(vertx, options);
    AtomicInteger count = new AtomicInteger();
    VertxEventHandler handler = new VertxEventHandler() {
      @Override
      public void handle(Event event, Future<Void> completeFuture) {
        logger.info("---| handle {}", event);
        count.incrementAndGet();
        vertx.setTimer(1000, l -> completeFuture.complete());
      }
    }.register(null, null);
//    try {
//      TimeUnit.SECONDS.sleep(30);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    consumer.close();
//    try {
//      TimeUnit.SECONDS.sleep(10);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }

//    System.out.println(consumer.metrics());
//    System.out.println(consumer.metrics().get("eventbus.consumer.completed"));
//    System.out.println(count);
  }


}
