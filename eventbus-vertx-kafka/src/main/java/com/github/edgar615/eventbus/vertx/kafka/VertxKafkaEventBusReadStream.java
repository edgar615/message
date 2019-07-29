package com.github.edgar615.eventbus.vertx.kafka;

import static net.logstash.logback.marker.Markers.appendEntries;

import com.github.edgar615.eventbus.event.Event;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.EventSerDe;
import com.github.edgar615.eventbus.utils.LoggingMarker;
import com.github.edgar615.eventbus.vertx.AbstractVertxEventBusReadStream;
import com.github.edgar615.eventbus.vertx.VertxEventConsumerRepository;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class VertxKafkaEventBusReadStream extends AbstractVertxEventBusReadStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxKafkaEventBusReadStream.class);

  private final KafkaConsumer<String, String> consumer;

  private final List<TopicPartition> partitionsAssigned = new CopyOnWriteArrayList<>();

  /**
   * 拉取消息的偏移量，用来手动提交时记录偏移
   */
  private final Map<TopicPartition, Long> pollOffsets = new ConcurrentHashMap<>();

  /**
   * 已提交的偏移量，用来手动提交是记录偏移，如果拉取的偏移>已提交的偏移，说明需要提交
   */
  private final Map<TopicPartition, Long> commitedOffsets = new ConcurrentHashMap<>();

  private final KafkaReadOptions options;

  private boolean enableAutoCommit;

  /**
   * 状态：0启动中 1-运行中 2-关闭
   */
  private volatile int state = 0;

  private static final int STATE_WAITING = 0;
  private static final int STATE_RUNNING = 1;
  private static final int STATE_CLOSED = 2;

  public VertxKafkaEventBusReadStream(Vertx vertx, EventQueue queue,
      VertxEventConsumerRepository consumerRepository,
      KafkaReadOptions options) {
    super(vertx, queue, consumerRepository);
    this.consumer = KafkaConsumer.create(vertx, options.getConfigs());
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
  public void start() {
    consumer.handler(record -> {
      Map<String, Object> extra = new HashMap<>();
      extra.put("topic", record.topic());
      extra.put("timestamp", record.timestamp());
      extra.put("partition", record.partition());
      extra.put("offset", record.offset());
      try {
        Event event = EventSerDe.deserialize(record.topic(), record.value());
        LOGGER.info(LoggingMarker.getLoggingMarker(event, true, extra), "poll from kafka");
        enqueue(Lists.newArrayList(event), ar -> {
          if (ar.failed()) {
            return;
          }
          int count = ar.result();
          // 手动提交，只要入队（DB）就认为消费了，消费失败或未正常消费的问题应该交由业务方处理
          if (!enableAutoCommit && count > 0) {
            commit();
          }
        });
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

    });
    partitionsAssigned();
    partitionsRevoked();
    subscribe();
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
        needCommitOffsets.put(topicPartition, new OffsetAndMetadata(pollOffset, null));
      }
    }
    consumer.commit(needCommitOffsets, ar -> {
      if (ar.failed()) {
        LOGGER.warn("commit failed", ar.cause());
        return;
      }
      for (TopicPartition topicPartition : ar.result().keySet()) {
        OffsetAndMetadata offsetAndMetadata = ar.result().get(topicPartition);
        commitedOffsets.put(topicPartition, offsetAndMetadata.getOffset());
        Map<String, Object> extra = new HashMap<>();
        extra.put("topic", topicPartition.getTopic());
        extra.put("partition", topicPartition.getPartition());
        extra.put("offset", offsetAndMetadata.getOffset());
        Marker messageMarker =
            appendEntries(extra);
        LOGGER.debug(messageMarker, "commit succeed");
      }
    });
  }

  private void subscribe() {
    if (options.getTopics().isEmpty()
        && !Strings.isNullOrEmpty(options.getPattern())) {
      LOGGER.info("subscribe with pattern:{}", options.getPattern());
      consumer.subscribe(Pattern.compile(options.getPattern()));
    } else {
      for (String topic : options.getTopics()) {
        LOGGER.info("subscribe:{}", topic);
      }
      consumer.subscribe(new HashSet<>(options.getTopics()));
    }
  }

  private void partitionsAssigned() {
    consumer.partitionsAssignedHandler(topicPartitions -> {
      partitionsAssigned.clear();
      List<Future> futures = new ArrayList<>();
      for (TopicPartition tp : topicPartitions) {
        Future<Void> future = Future.future();
        futures.add(future);
        partitionsAssigned.add(new TopicPartition(tp.getTopic(), tp.getPartition()));
        consumer.committed(tp, ar -> {
          if (ar.failed()) {
            return;
          }
          OffsetAndMetadata offsetAndMetadata = ar.result();
          Map<String, Object> extra = new HashMap<>();
          extra.put("topic", tp.getTopic());
          extra.put("partition", tp.getPartition());
//          extra.put("position", position);
          extra.put("lastCommitedOffsetAndMetadata", offsetAndMetadata);
          Marker messageMarker =
              appendEntries(extra);
          LOGGER.info(messageMarker, "partitionsAssigned");
          if (state == STATE_WAITING) {
            setStartOffset(tp, future);
          }
        });
      }
      CompositeFuture.all(futures)
          .setHandler(ar -> {
            state = STATE_RUNNING;
          });
    });
  }

  private void partitionsRevoked() {
    consumer.partitionsRevokedHandler(topicPartitions -> {
      for (TopicPartition tp : topicPartitions) {
        Map<String, Object> extra = new HashMap<>();
        extra.put("topic", tp.getTopic());
        extra.put("partition", tp.getPartition());
        Marker messageMarker =
            appendEntries(extra);
        LOGGER.info(messageMarker, "partitionsRevoked");
      }
    });
  }

  @Override
  public void close() {
    state = STATE_CLOSED;
    consumer.unsubscribe();
    consumer.close();
  }

//  private void startKafkaConsumer() {
//    try {
//      while (state != STATE_CLOSED) {
//        try {
//          long count = pollAndEnqueue();
//          // 手动提交，只要入队（DB）就认为消费了，消费失败或未正常消费的问题应该交由业务方处理
//          if (!enableAutoCommit && count > 0) {
//            commit();
//          }
//        } catch (Exception e) {
//          LOGGER.error("poll event from kafka occur error", e);
//        }
//
//      }
//    } catch (Exception e) {
//      LOGGER.error("consume from kafka occur error", e);
//    } finally {
//      consumer.close();
//      LOGGER.info("closing kafka consumer");
//    }
//  }

  private void setStartOffset(TopicPartition tp, Handler<AsyncResult<Void>> completionHandler) {
    long startingOffset = options.getStartingOffset(tp);
    Map<String, Object> extra = new HashMap<>();
    extra.put("topic", tp.getTopic());
    extra.put("partition", tp.getPartition());
    Handler<AsyncResult<Void>> handler = ar -> {
      if (ar.succeeded()) {
        Marker messageMarker =
            appendEntries(extra);
        LOGGER.info(messageMarker, "set kafka start offset");
      } else {
        Marker messageMarker =
            appendEntries(extra);
        LOGGER.error(messageMarker, "set kafka start offset failed", ar.cause());
      }
      completionHandler.handle(Future.succeededFuture());
    };
    if (startingOffset == -2) {
      extra.put("offset", "default");
    } else if (startingOffset == 0) {
      extra.put("offset", "beginning");
      consumer.seekToBeginning(tp, handler);
    } else if (startingOffset == -1) {
      extra.put("offset", "end");
      consumer.seekToEnd(tp, handler);
    } else {
      extra.put("offset", startingOffset);
      consumer.seek(tp, startingOffset, handler);
    }

  }

  @Override
  public boolean pause() {
    boolean result = super.pause();
    consumer.pause(new HashSet<>(partitionsAssigned), ar -> {
      if (ar.failed()) {
        super.resume();
      }
    });
    return result;
  }

  @Override
  public boolean resume() {
    boolean result = super.resume();
    consumer.resume(new HashSet<>(partitionsAssigned), ar -> {
      if (ar.failed()) {
        super.pause();
      }
    });
    return result;
  }
}
