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
import java.util.regex.Pattern;

public abstract class KafkaReadStream implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReadStream.class);

  private static final String LOG_TYPE = "eventbus-consumer";

  private final KafkaConsumerOptions options;

  private final ExecutorService consumerExecutor;

  private KafkaConsumer<String, Event> consumer;

  private volatile boolean started = false;

  private List<TopicPartition> partitionsAssigned = new CopyOnWriteArrayList<>();

  private volatile boolean pause = false;

  private volatile boolean closed = false;

  public KafkaReadStream(KafkaConsumerOptions options) {
    this.options = options;
    this.consumerExecutor =
            Executors.newFixedThreadPool(1, NamedThreadFactory.create("eventbus-consumer"));
    this.consumerExecutor.submit(this);
  }

  public abstract void handleEvents(List<Event> events);

  public abstract boolean checkPauseCondition();

  public abstract boolean checkResumeCondition();

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

  public void pause() {
    consumer.pause(partitionsAssigned.toArray(new TopicPartition[partitionsAssigned.size()]));
    pause = true;
    Log.create(LOGGER)
            .setLogType(LOG_TYPE)
            .setEvent("pause")
            .info();
  }

  public void resume() {
    consumer.resume(partitionsAssigned.toArray(new TopicPartition[partitionsAssigned.size()]));
    pause = false;
    Log.create(LOGGER)
            .setLogType(LOG_TYPE)
            .setEvent("resume")
            .info();
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
      while (!closed) {
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
      consumer.seekToBeginning(new TopicPartition[]{tp});
      Log.create(LOGGER)
              .setLogType(LOG_TYPE)
              .setEvent("StartingOffset")
              .addData("topic", tp.topic())
              .addData("partition", tp.partition())
              .addData("offset", "beginning")
              .info();
    } else if (startingOffset == -1) {
      consumer.seekToEnd(new TopicPartition[]{tp});
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
