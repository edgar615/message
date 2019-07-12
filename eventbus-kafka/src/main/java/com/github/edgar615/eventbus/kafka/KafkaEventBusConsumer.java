package com.github.edgar615.eventbus.kafka;

import com.github.edgar615.eventbus.event.Event;

import java.util.List;
import java.util.function.Function;

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
public class KafkaEventBusConsumer extends EventConsumerImpl {

  private final KafkaReadStream readStream;

  public KafkaEventBusConsumer(KafkaConsumerOptions options) {
    this(options, null, null, null);
  }

  public KafkaEventBusConsumer(KafkaConsumerOptions options, ConsumerStorage consumerStorage,
                            Function<Event, String> identificationExtractor,
                            Function<Event, Boolean> blackListFilter) {
    super(options, consumerStorage, identificationExtractor, blackListFilter);
    this.readStream = new KafkaReadStream(options) {
      @Override
      public void handleEvents(List<Event> events) {
        enqueue(events);
      }

      @Override
      public boolean checkPauseCondition() {
        return isFull();
      }

      @Override
      public boolean checkResumeCondition() {
        return isLowWaterMark();
      }
    };
  }

  @Override
  public void pause() {
    readStream.pause();
  }

  @Override
  public void resume() {
    readStream.resume();
  }

  @Override
  public void close() {
    super.close();
    readStream.close();
  }

  @Override
  public boolean isRunning() {
    return super.isRunning() && readStream.isRunning();
  }

  @Override
  public boolean paused() {
    return readStream.paused();
  }

}
