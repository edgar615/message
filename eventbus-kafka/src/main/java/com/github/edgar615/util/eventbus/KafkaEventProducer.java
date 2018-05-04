package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.log.Log;
import com.github.edgar615.util.log.LogType;
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

  private static final String LOG_TYPE = "eventbus-producer";

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
        Log.create(LOGGER)
                .setLogType(LogType.MS)
                .setEvent("kafka")
                .setTraceId(event.head().id())
                .setMessage("[{},{},{}] [{}] [{}] [{}]")
                .addArg(metadata.topic())
                .addArg(metadata.partition())
                .addArg(metadata.offset())
                .addArg(event.head().action())
                .addArg(Helper.toHeadString(event))
                .addArg(Helper.toActionString(event))
                .info();
        future.complete(null);
      } else {
        Log.create(LOGGER)
                .setLogType(LogType.MS)
                .setEvent("kafka")
                .setTraceId(event.head().id())
                .setMessage("[{},{},{}] [{}] [{}] [{}]")
                .addArg(metadata.topic())
                .addArg(metadata.partition())
                .addArg(metadata.offset())
                .addArg(event.head().action())
                .addArg(Helper.toHeadString(event))
                .addArg(Helper.toActionString(event))
                .setThrowable(future.cause())
                .error();
        future.fail(exception);
      }
    });
    return future;
  }
}
