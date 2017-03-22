package com.edgar.util.eventbus.kafka;

import com.edgar.util.eventbus.Eventbus;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class EventbusImpl implements Eventbus {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventbusImpl.class);

  private final ExecutorService consumeExecutor = Executors.newFixedThreadPool(1);

  private final ExecutorService sendExecutor = Executors.newFixedThreadPool(1);

  private Producer<String, String> producer;

  public EventbusImpl(KafkaOptions options) {

    this.producer = new KafkaProducer<>(options.producerProps());
    ConsumerRunnable runnable = new ConsumerRunnable();
    runnable.setClientId(options.getId());
    runnable.setGroupId(options.getGroup());
    runnable.setKafkaConnect("test.ihorn.com.cn:9092");
    options.getConsumerTopics().forEach(t -> {
      runnable.addTopic(t);
    });
//    runnable.setStartingOffset(44096);
    consumeExecutor.execute(runnable);
  }

}
