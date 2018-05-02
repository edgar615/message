package com.github.edgar615.util.eventbus;

import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.github.edgar615.util.concurrent.NamedThreadFactory;
import com.github.edgar615.util.event.Event;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
public class KafkaEventConsumer extends EventConsumerImpl implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventConsumer.class);

  private final KafkaConsumerOptions options;

  private final ExecutorService consumerExecutor;

  private KafkaConsumer<String, Event> consumer;

  private volatile boolean running = true;

  private volatile boolean started = false;

  private List<TopicPartition> partitionsAssigned = new CopyOnWriteArrayList<>();

  public KafkaEventConsumer(KafkaConsumerOptions options) {
    super(options);
    this.consumerExecutor =
            Executors.newFixedThreadPool(1, NamedThreadFactory.create("eventbus-consumer"));
    this.options = options;
    consumerExecutor.submit(this);
  }

  /**
   * commit完成的消息
   */
  private synchronized void commit(ConsumerRecords<String, Event> records) {
    //没有自动判断offset来提交
    if (records.isEmpty()) {
      return;
    }
    if (!options.isConsumerAutoCommit()) {
      consumer.commitAsync(
              (offsets, exception) -> {
                if (exception != null) {
                  LOGGER.error("[consumer] [commit: {}]", offsets, exception.getMessage(),
                          exception);
                } else {
                  if (!offsets.isEmpty()) {
                    LOGGER.info("[consumer] [commit: {}]", offsets);
                  }
                }
              });
    }
  }

  @Override
  public void run() {
    try {
      startConsumer();
    } catch (Exception e) {
      LOGGER.error("[consumer] [Starting]", e);
    }
  }

  public void close() {
    running = false;
    consumer.close();
  }

  private void startConsumer() {
    consumer = new KafkaConsumer<>(options.consumerProps());
    List<PartitionInfo> partitions;
    for (String topic : options.getTopics()) {
      while ((partitions = consumer.partitionsFor(topic)) == null) {
        try {
          LOGGER.info("[consumer] [topic {} since no metadata is available, wait 5s]",
                  topic);
          TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      LOGGER.info("[consumer] [topic:{} is available] [partitions:{}]",
              topic, partitions);
    }
    if (options.getTopics().isEmpty()
            && !Strings.isNullOrEmpty(options.getPattern())) {
      LOGGER.info(
              "[consumer] [subscribe pattern {}]",
              options.getPattern());
      consumer.subscribe(Pattern.compile(options.getPattern()), createListener());
    } else {
      LOGGER.info(
              "[consumer] [subscribe topic {}]",
              options.getTopics());
      consumer.subscribe(options.getTopics(), createListener());
    }

    try {
      while (running) {
        try {
          ConsumerRecords<String, Event> records = consumer.poll(100);
          if (records.count() > 0) {
            LOGGER.info(
                    "[consumer] [poll {} messages]",
                    records.count());
          }
          List<Event> events = new ArrayList<>();
          //将读取的消息全部写入队列
          for (ConsumerRecord<String, Event> record : records) {
            Event event = record.value();
            LOGGER.info("<====== [{}] [{},{},{}] [{}] [{}] [{}]",
                    event.head().id(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    event.head().action(),
                    Helper.toHeadString(event),
                    Helper.toActionString(event));
            events.add(event);
          }
          events.stream().filter(e -> blackListFilter.apply(e))
                  .forEach(e -> LOGGER.info("---| [{}] [BLACKLIST]", e.head().id()));
          List<Event> enqueEvents = events.stream().filter(e -> !blackListFilter.apply(e))
                  .collect(Collectors.toList());
          enqueue(enqueEvents);
          commit(records);
        } catch (Exception e) {
          LOGGER.error("[consumer] [ERROR]", e);
        }

      }
    } catch (Exception e) {
      LOGGER.error("[consumer] [ERROR]", e);
    } finally {
      LOGGER.warn("[consumer] [EXIT]");
    }
  }

  private ConsumerRebalanceListener createListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.info(
                "[consumer] [onPartitionsRevoked] [partitions:{}]",
                partitions);
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
          LOGGER.info(
                  "[consumer] [onPartitionsAssigned] [topic:{}, parition:{}, offset:{}, "
                          + "commited:{}]",
                  tp.topic(),
                  tp.partition(),
                  position,
                  lastCommitedOffsetAndMetadata);

          if (!started) {
            setStartOffset(tp);
          }
        }
        started = true;
      }
    };
  }
  public void pause() {
    super.pause();
    consumer.pause(partitionsAssigned);
    LOGGER.info(
            "[consumer] [pause]");
  }
  public void resume() {
    super.resume();
    consumer.resume(partitionsAssigned);
    LOGGER.info(
            "[consumer] [resume]");
  }

  private void setStartOffset(TopicPartition topicPartition) {
    long startingOffset = options.getStartingOffset(topicPartition);
    if (startingOffset == -2) {
      LOGGER.info(
              "[consumer] [StartingOffset] [topic:{}, parition:{}, offset:{}]",
              topicPartition.topic(),
              topicPartition.partition(),
              "none");
    } else if (startingOffset == 0) {
      consumer.seekToBeginning(Lists.newArrayList(topicPartition));
      LOGGER.info(
              "[consumer] [StartingOffset] [topic:{}, parition:{}, offset:{}]",
              topicPartition.topic(),
              topicPartition.partition(),
              "beginning");
    } else if (startingOffset == -1) {
      consumer.seekToEnd(Lists.newArrayList(topicPartition));
      LOGGER.info(
              "[consumer] [StartingOffset] [topic:{}, parition:{}, offset:{}]",
              topicPartition.topic(),
              topicPartition.partition(),
              "end");
    } else {
      consumer.seek(topicPartition, startingOffset);
      LOGGER.info(
              "[consumer] [StartingOffset] [topic:{}, parition:{}, offset:{}]",
              topicPartition.topic(),
              topicPartition.partition(),
              startingOffset);
    }
  }
}
