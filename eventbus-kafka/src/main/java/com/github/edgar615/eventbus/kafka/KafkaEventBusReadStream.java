package com.github.edgar615.eventbus.kafka;

import static net.logstash.logback.marker.Markers.appendEntries;

import com.github.edgar615.eventbus.bus.AbstractEventBusReadStream;
import com.github.edgar615.eventbus.repository.EventConsumerRepository;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
 * 一个或多个Consumer线程，Consumer除了读取消息外，还包括具体的业务逻辑处理，同一个Consumer线程里对事件串行处理， 每个事件完成之后再commit.
 * <p>
 * 同一个主题的线程数受限于主题的分区数，多余的线程不会接收任何消息。
 * <p>
 * 如果对消息的处理比较耗时，容易导致消费者的rebalance，因为如果在一段事件内没有收到Consumer的poll请求，会触发kafka的rebalance.
 * <p>
 * <p>
 * <b>维护一个或多个KafkaConsumer，同时维护多个事件处理线程(worker thread)</b>
 * 一个或者多个Consumer线程，Consumer只用来从kafka读取消息，并不涉及具体的业务逻辑处理, 具体的业务逻辑由Consumer转发给工作线程来处理.
 * <p>
 * 使用工作线程处理事件的时候，需要注意commit的正确的offset。 如果有两个工作线程处理事件，工作线程A，处理事件 1，工作线程B，处理事件2.
 * 如果工作线程的2先处理完，不能立刻commit。 否则有可能导致1的丢失.所以这种模式需要一个协调器来检测各个工作线程的消费状态，来对合适的offset进行commit
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

  /**
   * 状态：0启动中 1-运行中 2-关闭
   */
  private volatile int state = 0;

  private static final int STATE_WAITING = 0;
  private static final int STATE_RUNNING = 1;
  private static final int STATE_CLOSED = 2;

  private List<TopicPartition> partitionsAssigned = new CopyOnWriteArrayList<>();

  private final KafkaReadOptions options;

  private boolean enableAutoCommit;

  /**
   * 拉取消息的偏移量，用来手动提交时记录偏移
   */
  private final Map<TopicPartition, Long> pollOffsets = new ConcurrentHashMap<>();

  /**
   * 已提交的偏移量，用来手动提交是记录偏移，如果拉取的偏移>已提交的偏移，说明需要提交
   */
  private final Map<TopicPartition, Long> commitedOffsets = new ConcurrentHashMap<>();

  public KafkaEventBusReadStream(EventQueue queue,
      EventConsumerRepository consumerDao, KafkaReadOptions options) {
    super(queue, consumerDao);
    this.consumerExecutor =
        Executors.newFixedThreadPool(1, NamedThreadFactory.create("kafka-consumer"));
    this.consumer = new KafkaConsumer<>(options.getConfigs());
    this.options = options;
    Object enableAutoCommitConfig = options.getConfigs()
        .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    if (enableAutoCommitConfig == null) {
      enableAutoCommit = true;
    } else {
      enableAutoCommit = Boolean.parseBoolean(enableAutoCommitConfig.toString());
    }

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
      // 记录最近的偏移量，
      if (!enableAutoCommit) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        pollOffsets.put(topicPartition, record.offset());
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
    state = STATE_CLOSED;
    consumerExecutor.shutdown();
  }

  @Override
  public void pause() {
    this.consumer.pause(partitionsAssigned);
    super.pause();
  }

  @Override
  public void resume() {
    this.consumer.resume(partitionsAssigned);
    super.resume();
  }

  @Override
  public void run() {
    startKafkaConsumer();
  }

  private void startKafkaConsumer() {
    List<PartitionInfo> partitions;
    for (String topic : options.getTopics()) {
      while ((partitions = consumer.partitionsFor(topic)) == null) {
        try {
          LOGGER.warn("topic:{} not found ,wait {}s", topic, 5);
          TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      LOGGER.info("topic:{} available ,partitions:{}", topic, partitions);
    }
    if (options.getTopics().isEmpty()
        && !Strings.isNullOrEmpty(options.getPattern())) {
      LOGGER.info("subscribe with pattern:{}", options.getPattern());
      consumer.subscribe(Pattern.compile(options.getPattern()), createListener());
    } else {
      for (String topic : options.getTopics()) {
        LOGGER.info("subscribe:{}", topic);
      }
      consumer.subscribe(options.getTopics(), createListener());
    }

    try {
      while (state != STATE_CLOSED) {
        try {
          long count = pollAndEnqueue();
          // 手动提交，只要入队（DB）就认为消费了，消费失败或未正常消费的问题应该交由业务方处理
          if (!enableAutoCommit && count > 0) {
            commit();
          }
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
  private synchronized void commit() {
    //没有自动判断offset来提交
    // 成功提交的分区要从offset清理掉
    Map<TopicPartition, OffsetAndMetadata> needCommitOffsets = new HashMap<>();
    for (TopicPartition topicPartition : pollOffsets.keySet()) {
      long pollOffset = pollOffsets.get(topicPartition);
      if (!commitedOffsets.containsKey(topicPartition)
          || commitedOffsets.get(topicPartition) < pollOffset) {
        needCommitOffsets.put(topicPartition, new OffsetAndMetadata(pollOffset));
      }
    }

    consumer.commitAsync(
        (offsets, exception) -> {
          for (TopicPartition topicPartition : offsets.keySet()) {
            commitedOffsets.put(topicPartition, offsets.get(topicPartition).offset());
            Map<String, Object> extra = new HashMap<>();
            extra.put("topic", topicPartition.topic());
            extra.put("partition", topicPartition.partition());
            extra.put("offset", offsets.get(topicPartition).offset());
            Marker messageMarker =
                appendEntries(extra);
            if (exception != null) {
              LOGGER.warn(messageMarker, "commit failed", exception);
            } else {
              LOGGER.debug(messageMarker, "commit succeed");
            }
          }
        });
  }

  private ConsumerRebalanceListener createListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
          Map<String, Object> extra = new HashMap<>();
          extra.put("topic", tp.topic());
          extra.put("partition", tp.partition());
          Marker messageMarker =
              appendEntries(extra);
          LOGGER.info(messageMarker, "partitionsRevoked");
        }
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
          Map<String, Object> extra = new HashMap<>();
          extra.put("topic", tp.topic());
          extra.put("partition", tp.partition());
          extra.put("position", position);
          extra.put("lastCommitedOffsetAndMetadata", lastCommitedOffsetAndMetadata);
          Marker messageMarker =
              appendEntries(extra);
          LOGGER.info(messageMarker, "partitionsAssigned");
          if (state == STATE_WAITING) {
            setStartOffset(tp);
          }
        }
        state = STATE_RUNNING;
      }
    };
  }

  private void setStartOffset(TopicPartition tp) {
    long startingOffset = options.getStartingOffset(tp);
    Map<String, Object> extra = new HashMap<>();
    extra.put("topic", tp.topic());
    extra.put("partition", tp.partition());
    if (startingOffset == -2) {
      extra.put("offset", "default");
    } else if (startingOffset == 0) {
      extra.put("offset", "beginning");
      consumer.seekToBeginning(Lists.newArrayList(tp));
    } else if (startingOffset == -1) {
      extra.put("offset", "end");
      consumer.seekToEnd(Lists.newArrayList(tp));
    } else {
      extra.put("offset", startingOffset);
      consumer.seek(tp, startingOffset);
    }
    Marker messageMarker =
        appendEntries(extra);
    LOGGER.info(messageMarker, "set kafka start offset");
  }
}
