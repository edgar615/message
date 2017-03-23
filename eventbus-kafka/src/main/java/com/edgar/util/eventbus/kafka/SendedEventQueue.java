package com.edgar.util.eventbus.kafka;

import com.edgar.util.eventbus.event.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class SendedEventQueue {

  private final Runnable runnable;

  private ExecutorService executor = Executors.newFixedThreadPool(1);

  private LinkedList<Event> events = new LinkedList<>();

  private boolean running = false;

  private Producer<String, Event> producer;

  private AtomicInteger succeed = new AtomicInteger();

  private AtomicInteger failed = new AtomicInteger();

  private AtomicInteger proc = new AtomicInteger();

  public SendedEventQueue() {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.11.0.31:9092");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.edgar.util.eventbus"
                                                                    + ".kafka.EventSerializer");
//      producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionClass);
    this.producer = new KafkaProducer<>(producerProps);

    this.runnable = () -> {
      for (; ; ) {
        final Event event;
        synchronized (events) {
          event = events.poll();
          if (event == null) {
            running = false;
            return;
          }
        }

        try {
          handle(event);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
  }


  public void handle(Event event) throws InterruptedException {
//    event.getHead().setFrom(id);
//    event.getHead().setGroup(group);
    proc.incrementAndGet();
    ProducerRecord<String, Event> record =
            new ProducerRecord<>(event.head().to(), event);
    producer.send(record, (metadata, exception) -> {
      if (exception == null) {
        succeed.incrementAndGet();
        System.out.println(metadata);
      } else {
        failed.incrementAndGet();
        System.out.println(exception);
      }
      System.out.println("proc:" + proc.get() + ",waited:" + events.size() + ",succeed:" +
                         succeed.get() + ", failed:" + failed.get());
    });
    producer.flush();
  }

  public void enqueue(Event event) {
    synchronized (events) {
      if (events.size() > 10000) {
        //TODO
      }
      System.out.println(events.size());
      events.add(event);
      if (!running) {
        running = true;
        executor.execute(runnable);
      }
    }
  }

}
