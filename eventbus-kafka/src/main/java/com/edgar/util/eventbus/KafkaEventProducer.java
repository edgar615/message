package com.edgar.util.eventbus;

import com.edgar.util.event.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by Edgar on 2017/4/19.
 *
 * @author Edgar  Date 2017/4/19
 */
public class KafkaEventProducer extends EventProducerImpl {

  private Producer<String, Event> producer;

  KafkaEventProducer(KafkaProducerOptions options) {
    super(options);
    producer = new KafkaProducer<>(options.toProps());
  }


  @Override
  public EventFuture<Void> sendEvent(Event event) {
    EventFuture<Void> future = EventFuture.future(event);
    ProducerRecord<String, Event> record =
            new ProducerRecord<>(event.head().to(), event);
    producer.send(record, (metadata, exception) -> {
      if (exception == null) {
        future.complete(null);
      } else {
        future.fail(exception);
      }
    });
    return future;
  }
}
