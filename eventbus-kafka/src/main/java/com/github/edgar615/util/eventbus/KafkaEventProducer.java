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
    this(options, null);
  }

  public KafkaEventProducer(KafkaProducerOptions options, ProducerStorage producerStorage) {
    super(options, producerStorage);
    producer = new KafkaProducer<>(options.toProps());
  }

  @Override
  public EventFuture sendEvent(Event event) {
    EventFuture future = EventFuture.future(event);
    ProducerRecord<String, Event> record =
            new ProducerRecord<>(event.head().to(), event);
    producer.send(record, (metadata, exception) -> {
      if (exception == null) {
        LOGGER.info("[{}] [MS] [KAFKA] [OK] [{},{},{}] [{}] [{}]", event.head().id(),
                metadata.topic(),metadata.partition(),metadata.offset(),
                Helper.toHeadString(event),
                Helper.toActionString(event));
        future.complete();
      } else {
        LOGGER.error("[{}] [MS] [KAFKA] [FAILED] [{}] [{}] [{}]", event.head().id(),
                event.head().to(),
                Helper.toHeadString(event),
                Helper.toActionString(event), exception);
        future.fail(exception);
      }
    });
    return future;
  }

  @Override
  public void close() {
    super.close();
    producer.close();
  }
}
