package com.edgar.util.eventbus.kafka;

import com.edgar.util.eventbus.EventSendAction;
import com.edgar.util.eventbus.Eventbus;
import com.edgar.util.eventbus.SendedQueue;
import com.edgar.util.eventbus.event.Event;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Edgar  Date 2017/3/22
 */
public class EventbusImpl implements Eventbus {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventbusImpl.class);

  private final ExecutorService consumeExecutor = Executors.newFixedThreadPool(1);

  private final ThreadPoolExecutor sendExecutor =
          (ThreadPoolExecutor) Executors.newFixedThreadPool(5);

  private final SendedQueue sendedQueue;

  Producer<String, Event> producer;

  public EventbusImpl(KafkaEventbusOptions options) {
//    http://kelgon.iteye.com/blog/2287985
    ProducerOptions producerOptions = new ProducerOptions()
            .setServers("10.11.0.31:9092");
    EventSendAction action = new KafkaSendAction(producerOptions);
    sendedQueue = SendedQueue.create(action, 10000);
//    ConsumerRunnable runnable = new ConsumerRunnable();
//    runnable.setClientId(options.getId());
//    runnable.setGroupId(options.getGroup());
//    runnable.setKafkaConnect(options.getServers());
//    options.getConsumerTopics().forEach(t -> {
//      runnable.addTopic(t);
//    });
//    runnable.setStartingOffset(-1);
//    consumeExecutor.execute(runnable);
  }

  @Override
  public void send(Event event) {
    sendedQueue.enqueue(event);
  }

}
