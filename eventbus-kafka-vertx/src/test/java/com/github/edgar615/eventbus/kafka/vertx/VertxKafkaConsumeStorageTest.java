package com.github.edgar615.eventbus.kafka.vertx;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.kafka.KafkaReadOptions;
import com.github.edgar615.eventbus.vertx.VertxEventConsumerRepository;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class VertxKafkaConsumeStorageTest {

  private static Logger logger = LoggerFactory.getLogger(VertxKafkaConsumeStorageTest.class);

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();
    String server = "120.76.158.7:9092";
    KafkaReadOptions options = new KafkaReadOptions();
    options.setServers(server)
            .setGroup("test-ezere")
//            .setPattern(".*")
            .addTopic("DeviceControlEvent_1_3")
            .setMaxPollRecords(1)
            .setMaxQuota(5)
            .setConsumerAutoOffsetRest("earliest");
    AtomicInteger total = new AtomicInteger();
    AtomicInteger black = new AtomicInteger();
    AtomicInteger consumed = new AtomicInteger();
    Multimap<Integer, Event> storage = ArrayListMultimap.create();
    KafkaVertxEventbusConsumer consumer
            = new KafkaVertxEventbusConsumerImpl(vertx, options,
                                                 new VertxEventConsumerRepository() {
                                                   @Override
                                                   public boolean
                                                   shouldStorage(Event event) {
                                                     return true;
                                                   }

                                                   @Override
                                                   public void
                                                   isConsumed(Event e,
                                                              Handler<AsyncResult<Boolean>>
                                                                      resultHandler) {
                                                     total.incrementAndGet();
                                                     boolean result =
                                                             Integer.parseInt(e.action().resource())
                                                             % 3 == 0;
                                                     if (result) {
                                                       consumed.incrementAndGet();
                                                     }
                                                     resultHandler.handle(Future.succeededFuture
                                                             (result));
                                                   }

                                                   @Override
                                                   public void mark(Event event,
                                                                    int result,
                                                                    Handler<AsyncResult<Void>>
                                                                            resultHandler) {
                                                      storage.put(result, event);
                                                     resultHandler.handle(Future.succeededFuture());
                                                   }
                                                 },

                                                 null, e -> {
      boolean result = Integer.parseInt(e.action().resource()) % 5 == 0;
      if (result) {
        black.incrementAndGet();
      }
      return result;
    });
    AtomicInteger count = new AtomicInteger();
    VertxEventHandler handler = new VertxEventHandler() {
      @Override
      public void handle(Event event, Future<Void> completeFuture) {
        logger.info("---| handle {}", event);
        count.incrementAndGet();
        if (Integer.parseInt(event.action().resource()) % 2 == 0) {
          vertx.setTimer(1000, l -> completeFuture.fail("aa"));
        } else {
          vertx.setTimer(1000, l -> completeFuture.complete());
        }

      }
    }.register(null, null);
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

//    System.out.println(consumer.metrics());
//    System.out.println(consumer.metrics().get("kafka.consumer.completed"));
    System.out.println(count);
    System.out.println(black);
    System.out.println(total);
    System.out.println(consumed);
    System.out.println(storage.get(1).size());
    System.out.println(storage.get(2).size());
    System.out.println(storage.get(3).size());
    System.out.println(consumer.metrics());
  }


}
