package com.edgar.util.eventbus.kafka;

import com.edgar.util.eventbus.Eventbus;
import com.edgar.util.eventbus.event.Event;
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

  private final SendedEventQueue sendedEventQueue;

  public EventbusImpl(KafkaOptions options) {
//    http://kelgon.iteye.com/blog/2287985
    sendedEventQueue = new SendedEventQueue();
    ConsumerRunnable runnable = new ConsumerRunnable();
    runnable.setClientId(options.getId());
    runnable.setGroupId(options.getGroup());
    runnable.setKafkaConnect(options.getServers());
    options.getConsumerTopics().forEach(t -> {
      runnable.addTopic(t);
    });
//    runnable.setStartingOffset(-1);
    consumeExecutor.execute(runnable);
  }

  @Override
  public void send(Event event) {

    sendedEventQueue.enqueue(event);
  }

}
