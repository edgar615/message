package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2017/4/19.
 *
 * @author Edgar  Date 2017/4/19
 */
public class KafkaEventProducer extends EventProducerImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventProducer.class);

  private Producer<String, Event> producer;

  public KafkaEventProducer(KafkaProducerOptions options) {
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
        LOGGER.info("======> [{}] [OK] [{},{},{}] [{}] [{}] [{}]",
                    event.head().id(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    event.head().action(),
                    Helper.toHeadString(event),
                    Helper.toActionString(event));
        future.complete(null);
      } else {
        LOGGER.error("======> [{}] [FAILED] [{}] [{}] [{}] [{}]",
                     event.head().id(),
                     event.head().to(),
                     event.head().action(),
                     Helper.toHeadString(event),
                     Helper.toActionString(event),
                     future.cause());
        future.fail(exception);
      }
    });
    return future;
  }
}
