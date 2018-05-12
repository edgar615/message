package com.github.edgar615.util.eventbus;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import com.github.edgar615.util.concurrent.NamedThreadFactory;
import com.github.edgar615.util.event.Event;
import com.github.edgar615.util.log.Log;
import com.github.edgar615.util.log.LogType;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

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

  private static final String LOG_TYPE = "eventbus-consumer";

  private final KafkaConsumerOptions options;

  private final ExecutorService consumerExecutor;

  private KafkaConsumer<String, Event> consumer;

  private volatile boolean started = false;

  private List<TopicPartition> partitionsAssigned = new CopyOnWriteArrayList<>();

  private volatile boolean pause = false;

  public KafkaEventConsumer(KafkaConsumerOptions options) {
    this(options, null, null, null);

  }

  public KafkaEventConsumer(KafkaConsumerOptions options, ConsumerStorage consumerStorage,
                            Function<Event, String> identificationExtractor,
                            Function<Event, Boolean> blackListFilter) {
    super(options, consumerStorage, identificationExtractor, blackListFilter);
    this.options = options;
    this.consumerExecutor =
            Executors.newFixedThreadPool(1, NamedThreadFactory.create("eventbus-consumer"));
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
                  Log.create(LOGGER)
                          .setLogType(LOG_TYPE)
                          .setEvent("commit")
                          .addData("offsets", offsets)
                          .setThrowable(exception)
                          .error();
                } else {
                  if (!offsets.isEmpty()) {
                    Log.create(LOGGER)
                            .setLogType(LOG_TYPE)
                            .setEvent("commit")
                            .addData("offsets", offsets)
                            .info();
                  }
                }
              });
    }
  }

  @Override
  public void run() {
    try {
      startKafkaConsumer();
    } catch (Exception e) {
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("start.failed")
              .setThrowable(e)
              .error();
    }
  }

  public void subscribe(String topic) {
    consumer.subscribe(Lists.newArrayList(topic), createListener());
  }

  public void startKafkaConsumer() {
    consumer = new KafkaConsumer<>(options.consumerProps());
    List<PartitionInfo> partitions;
    for (String topic : options.getTopics()) {
      while ((partitions = consumer.partitionsFor(topic)) == null) {
        try {
          Log.create(LOGGER)
                  .setLogType(LOG_TYPE)
                  .setEvent("metadata.unavailable")
                  .addData("topic", topic)
                  .setMessage("wait 5s")
                  .info();
          TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("metadata.available")
              .addData("topic", topic)
              .addData("partitions", partitions)
              .setMessage("wait 5s")
              .info();
    }
    if (options.getTopics().isEmpty()
        && !Strings.isNullOrEmpty(options.getPattern())) {
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("subscribe")
              .addData("pattern", options.getPattern())
              .info();
      consumer.subscribe(Pattern.compile(options.getPattern()), createListener());
    } else {
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("subscribe")
              .addData("topics", options.getTopics())
              .info();
      consumer.subscribe(options.getTopics(), createListener());
    }

    try {
      while (isRunning()) {
        try {
          ConsumerRecords<String, Event> records = consumer.poll(100);
          if (records.count() > 0) {
            Log.create(LOGGER)
                    .setLogType(LOG_TYPE)
                    .setEvent("poll")
                    .addData("count", records.count())
                    .info();
          }
          List<Event> events = new ArrayList<>();
          //将读取的消息全部写入队列
          for (ConsumerRecord<String, Event> record : records) {
            Event event = record.value();
            Log.create(LOGGER)
                    .setLogType(LogType.MR)
                    .setEvent("kafka")
                    .setTraceId(event.head().id())
                    .setMessage("[{},{},{}] [{}] [{}] [{}]")
                    .addArg(record.topic())
                    .addArg(record.partition())
                    .addArg(record.offset())
                    .addArg(event.head().action())
                    .addArg(Helper.toHeadString(event))
                    .addArg(Helper.toActionString(event))
                    .info();
            events.add(event);
          }
          enqueue(events);
          commit(records);
          //暂停和恢复
          if (pause) {
            if (!isFull()) {
              resume();
            }
          } else {
            if (isFull()) {
              pause();
            }
          }
        } catch (Exception e) {
          Log.create(LOGGER)
                  .setLogType(LOG_TYPE)
                  .setEvent("ERROR")
                  .setThrowable(e)
                  .error();
        }

      }
    } catch (Exception e) {
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("ERROR")
              .setThrowable(e)
              .error();
    } finally {
      consumer.close();
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("consumer.exited")
              .info();
    }
  }

  public void pause() {
    consumer.pause(partitionsAssigned);
    pause = true;
    Log.create(LOGGER)
            .setLogType(LOG_TYPE)
            .setEvent("pause")
            .info();
  }

  public void resume() {
    consumer.resume(partitionsAssigned);
    pause = false;
    Log.create(LOGGER)
            .setLogType(LOG_TYPE)
            .setEvent("resume")
            .info();
  }

  @Override
  public void close() {
    super.close();
    consumerExecutor.shutdown();
  }

  private ConsumerRebalanceListener createListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Log.create(LOGGER)
                .setLogType(LOG_TYPE)
                .setEvent("onPartitionsRevoked")
                .addData("partitions", partitions)
                .info();
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
          Log.create(LOGGER)
                  .setLogType(LOG_TYPE)
                  .setEvent("onPartitionsAssigned")
                  .addData("topic", tp.topic())
                  .addData("partition", tp.partition())
                  .addData("offset", position)
                  .addData("partitions", partitions)
                  .addData("commited", lastCommitedOffsetAndMetadata)
                  .info();

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
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("StartingOffset")
              .addData("topic", tp.topic())
              .addData("partition", tp.partition())
              .addData("offset", "none")
              .info();
    } else if (startingOffset == 0) {
      consumer.seekToBeginning(Lists.newArrayList(tp));
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("StartingOffset")
              .addData("topic", tp.topic())
              .addData("partition", tp.partition())
              .addData("offset", "beginning")
              .info();
    } else if (startingOffset == -1) {
      consumer.seekToEnd(Lists.newArrayList(tp));
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("StartingOffset")
              .addData("topic", tp.topic())
              .addData("partition", tp.partition())
              .addData("offset", "end")
              .info();
    } else {
      consumer.seek(tp, startingOffset);
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("StartingOffset")
              .addData("topic", tp.topic())
              .addData("partition", tp.partition())
              .addData("offset", startingOffset)
              .info();
    }
  }
}
