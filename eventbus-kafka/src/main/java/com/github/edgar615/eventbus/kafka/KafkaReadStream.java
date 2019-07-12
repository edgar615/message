package com.github.edgar615.eventbus.kafka;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.NamedThreadFactory;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.*;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public abstract class KafkaReadStream implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReadStream.class);

  private final KafkaConsumerOptions options;

  private final ExecutorService consumerExecutor;

  private KafkaConsumer<String, Event> consumer;

  private volatile boolean started = false;

  private List<TopicPartition> partitionsAssigned = new CopyOnWriteArrayList<>();

  private volatile boolean pause = false;

  private volatile boolean closed = false;

  private final AtomicInteger pauseCount = new AtomicInteger();

  private volatile long latestPaused = 0L;

  public KafkaReadStream(KafkaConsumerOptions options) {
    this.options = options;
    this.consumerExecutor =
            Executors.newFixedThreadPool(1, NamedThreadFactory.create("kafka-consumer"));
    this.consumerExecutor.submit(this);
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
                  LOGGER.error("[KAFKA][commitFailed] [{}]", offsets, exception);
                } else {
                  if (!offsets.isEmpty()) {
                    LOGGER.debug("[KAFKA] [committed] [{}]", offsets);
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
      LOGGER.error("[KAFKA] [startFailed]", e);
    }
  }

  public abstract void handleEvents(List<Event> events);

  public abstract boolean checkPauseCondition();

  public abstract boolean checkResumeCondition();

  public void subscribe(String topic) {
    consumer.subscribe(Lists.newArrayList(topic), createListener());
  }

  public void pause() {
    consumer.pause(partitionsAssigned);
    pause = true;
    int count = pauseCount.incrementAndGet();
    latestPaused = System.currentTimeMillis();
    LOGGER.info("[KAFKA] [pause] [{}]", count);
  }

  public void resume() {
    consumer.resume(partitionsAssigned);
    pause = false;
    LOGGER.info("[KAFKA] [resume] [{}ms]", System.currentTimeMillis() - latestPaused);
  }

  public void close() {
    closed = true;
//    consumer.close();
    consumerExecutor.shutdown();
  }

  public boolean isRunning() {
    return !closed;
  }

  public boolean paused() {
    return pause;
  }

  private void startKafkaConsumer() {
    consumer = new KafkaConsumer<>(options.toProps());
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
          ConsumerRecords<String, Event> records = consumer.poll(100);
          if (records.count() > 0) {
            LOGGER.info("[KAFKA] [poll] [{} records]", records.count());
          }
          List<Event> events = new ArrayList<>();
          //将读取的消息全部写入队列
          for (ConsumerRecord<String, Event> record : records) {
            Event event = record.value();
            LOGGER.info("[{}] [MR] [KAFKA] [{},{},{}] [{}] [{}] [{}]", event.head().id(),
                    record.topic(), record.partition(), record.offset(),
                    Helper.toHeadString(event), Helper.toActionString(event));
            events.add(event);
          }
          handleEvents(events);
          commit(records);
          //暂停和恢复
          if (pause) {
            //队列中等待的消息降到一半才恢复
            if (checkResumeCondition()) {
              resume();
            }
          } else {
            if (checkPauseCondition()) {
              pause();
            }
          }
        } catch (Exception e) {
          LOGGER.error("[KAFKA] [pollFailed]", e);
        }

      }
    } catch (Exception e) {
      LOGGER.error("[KAFKA] [failed]", e);
    } finally {
      consumer.close();
      LOGGER.info("[KAFKA] [exited]");
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
