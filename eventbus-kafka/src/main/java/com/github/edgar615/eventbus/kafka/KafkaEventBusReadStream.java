package com.github.edgar615.eventbus.kafka;

import static net.logstash.logback.marker.Markers.append;
import static net.logstash.logback.marker.Markers.appendEntries;

import com.github.edgar615.eventbus.bus.AbstractEventBusReadStream;
import com.github.edgar615.eventbus.dao.EventConsumerDao;
import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.EventSerDe;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import com.github.edgar615.eventbus.utils.NamedThreadFactory;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

/**
 * kafka的consumer对象不是线程安全的，如果在不同的线程里使用consumer会抛出异常.
 * <p>
 * 消息的消费有两种方式：每个线程维护一个KafkaConsumer 或者 维护一个或多个KafkaConsumer，同时维护多个事件处理线程(worker thread)
 * <p>
 * <b>每个线程维护一个KafkaConsumer</b>
 * 一个或多个Consumer线程，Consumer除了读取消息外，还包括具体的业务逻辑处理，同一个Consumer线程里对事件串行处理，
 * 每个事件完成之后再commit.
 * <p>
 * 同一个主题的线程数受限于主题的分区数，多余的线程不会接收任何消息。
 * <p>
 * 如果对消息的处理比较耗时，容易导致消费者的rebalance，因为如果在一段事件内没有收到Consumer的poll请求，会触发kafka的rebalance.
 * <p>
 * <p>
 * <b>维护一个或多个KafkaConsumer，同时维护多个事件处理线程(worker thread)</b>
 * 一个或者多个Consumer线程，Consumer只用来从kafka读取消息，并不涉及具体的业务逻辑处理, 具体的业务逻辑由Consumer转发给工作线程来处理.
 * <p>
 * 使用工作线程处理事件的时候，需要注意commit的正确的offset。
 * 如果有两个工作线程处理事件，工作线程A，处理事件 1，工作线程B，处理事件2. 如果工作线程的2先处理完，不能立刻commit。
 * 否则有可能导致1的丢失.所以这种模式需要一个协调器来检测各个工作线程的消费状态，来对合适的offset进行commit
 * <p>
 * <p>
 * Eventbus采用第二种方案消费消息.
 *
 * @author Edgar  Date 2017/4/5
 */
public class KafkaEventBusReadStream extends AbstractEventBusReadStream implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventBusReadStream.class);

  private final ExecutorService consumerExecutor;

  private final KafkaConsumer<String, String> consumer;

  private volatile boolean closed = false;

  private List<TopicPartition> partitionsAssigned = new CopyOnWriteArrayList<>();

  public KafkaEventBusReadStream(EventQueue queue,
      EventConsumerDao consumerDao, Map<String, Object> configs) {
    super(queue, consumerDao);
    this.consumerExecutor =
        Executors.newFixedThreadPool(1, NamedThreadFactory.create("kafka-consumer"));
    this.consumer = new KafkaConsumer<String, String>(configs);
  }

  @Override
  public List<Event> poll() {
    ConsumerRecords<String, String> records = consumer.poll(100);
    List<Event> events = new ArrayList<>();
    for (ConsumerRecord<String, String> record : records) {
      Map<String, Object> extra = new HashMap<>();
      extra.put("topic", record.topic());
      extra.put("timestamp", record.timestamp());
      extra.put("partition", record.partition());
      extra.put("offset", record.offset());
      try {
        Event event = EventSerDe.deserialize(record.topic(), record.value());
        LOGGER.info(LoggingMarker.getLoggingMarker(event, true, extra), "poll from kafka");
        events.add(event);
      } catch (Exception e) {
        Marker messageMarker =
            appendEntries(extra);
        LOGGER.warn(messageMarker, "poll from kafka, bus deserialize failed");
      }
    }
    return events;
  }

  @Override
  public void start() {
    this.consumerExecutor.submit(this);
  }

  @Override
  public void close() {
    closed = true;
    consumerExecutor.shutdown();
  }

  @Override
  public void pause() {
    this.consumer.pause(partitionsAssigned);
    super.pause();
  }

  @Override
  public void resume() {
    this.resume();
    super.resume();
  }

  @Override
  public void run() {

  }

  private void startKafkaConsumer() {
    List<PartitionInfo> partitions;
    for (String topic : options.getTopics()) {
      while ((partitions = consumer.partitionsFor(topic)) == null) {
        try {
          LOGGER.warn("[KAFKA] [waitMetadata] [{}]", topic);
          TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      LOGGER.info("[KAFKA] [available] [{}] [{}]", topic, partitions);
    }
    if (options.getTopics().isEmpty()
        && !Strings.isNullOrEmpty(options.getPattern())) {
      LOGGER.info("[KAFKA] [subscribe] [{}]", options.getPattern());
      consumer.subscribe(Pattern.compile(options.getPattern()), createListener());
    } else {
      LOGGER.info("[KAFKA] [subscribe] [{}]", options.getTopics());
      consumer.subscribe(options.getTopics(), createListener());
    }

    try {
      while (!closed) {
        try {

          //暂停和恢复
          if (paused()) {
            //队列中等待的消息降到一半才恢复
            if (checkResumeCondition()) {
              resume();
            }
          } else {
            if (checkPauseCondition()) {
              pause();
            }
          }
          pollAndEnqueue();
        } catch (Exception e) {
          LOGGER.error("poll event from kafka occur error", e);
        }

      }
    } catch (Exception e) {
      LOGGER.error("consume from kafka occur error", e);
    } finally {
      consumer.close();
      LOGGER.info("closing kafka consumer");
    }
  }


  /**
   * commit完成的消息
   */
  private synchronized void commit(ConsumerRecords<String, String> records) {
    //没有自动判断offset来提交
    if (records.isEmpty()) {
      return;
    }
    if (!options.isConsumerAutoCommit()) {
      consumer.commitAsync(
          (offsets, exception) -> {
            if (exception != null) {
              LOGGER.error("[KAFKA][commitFailed] [{}]", offsets, exception);
            } else {
              if (!offsets.isEmpty()) {
                LOGGER.debug("[KAFKA] [committed] [{}]", offsets);
              }
            }
          });
    }
  }

  private ConsumerRebalanceListener createListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.info("[KAFKA] [partitionsRevoked] [{}]", partitions);
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        partitionsAssigned.clear();
        Iterator<TopicPartition> topicPartitionIterator = partitions.iterator();
        while (topicPartitionIterator.hasNext()) {
          TopicPartition tp = topicPartitionIterator.next();
          partitionsAssigned.add(new TopicPartition(tp.topic(), tp.partition()));
          long position = consumer.position(tp);
          OffsetAndMetadata lastCommitedOffsetAndMetadata = consumer.committed(tp);
          LOGGER.info("[KAFKA] [partitionsAssigned] [{},{},{}] [{}] [{}]",
              tp.topic(), tp.partition(), position,
              partitions, lastCommitedOffsetAndMetadata);
          if (!started) {
            setStartOffset(tp);
          }
        }
        started = true;
      }
    };
  }

  private void setStartOffset(TopicPartition tp) {
    long startingOffset = options.getStartingOffset(tp);
    if (startingOffset == -2) {
      LOGGER.info("[KAFKA] [StartingOffset] [{},{},{}]",
          tp.topic(), tp.partition(), "none");
    } else if (startingOffset == 0) {
      consumer.seekToBeginning(Lists.newArrayList(tp));
      LOGGER.info("[KAFKA] [StartingOffset] [{},{},{}]",
          tp.topic(), tp.partition(), "beginning");
    } else if (startingOffset == -1) {
      consumer.seekToEnd(Lists.newArrayList(tp));
      LOGGER.info("[KAFKA] [StartingOffset] [{},{},{}]",
          tp.topic(), tp.partition(), "end");
    } else {
      consumer.seek(tp, startingOffset);
      LOGGER.info("[KAFKA] [StartingOffset] [{},{},{}]",
          tp.topic(), tp.partition(), startingOffset);
    }
  }
}
