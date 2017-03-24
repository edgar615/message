package com.edgar.util.eventbus.kafka;

import com.edgar.util.eventbus.EventFuture;
import com.edgar.util.eventbus.EventSendAction;
import com.edgar.util.eventbus.event.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by Edgar on 2017/3/24.
 *
 * @author Edgar  Date 2017/3/24
 */
public class KafkaSendAction implements EventSendAction {

  private Producer<String, Event> producer;

  public KafkaSendAction(ProducerOptions options) {
    producer = new KafkaProducer<>(options.toProps());
  }

  @Override
  public EventFuture<Void> send(Event event) {
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
