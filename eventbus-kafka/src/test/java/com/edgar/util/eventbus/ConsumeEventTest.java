package com.edgar.util.eventbus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class ConsumeEventTest extends EventbusTest {

  private static Logger logger = LoggerFactory.getLogger(ConsumeEventTest.class);

  public static void main(String[] args) {
    String server = "10.11.0.31:9092";
    ConsumerOptions options = new ConsumerOptions();
    options.setServers(server)
            .setGroup("test-consumer")
            .addTopic("test");
    Eventbus eventbus = new EventbusImpl(null, options);
    eventbus.consumer(null, null, e-> {
      logger.info("---} handle {}", e);
      if (e.action().resource().equals("5")) {
        try {
          TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
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
