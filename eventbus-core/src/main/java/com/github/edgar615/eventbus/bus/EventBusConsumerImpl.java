package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.repository.EventConsumerRepository;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.NamedThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
class EventBusConsumerImpl implements EventBusConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventBusConsumer.class);

  private final ExecutorService workerExecutor;

  private final BlockedEventChecker checker;

  private final EventQueue eventQueue;

  private volatile boolean running = false;

  private EventConsumerRepository consumerRepository;

  private final int workerCount;

  private final long blockedCheckerMs;

  EventBusConsumerImpl(ConsumerOptions options, EventQueue queue,
      EventConsumerRepository consumerRepository) {
    this.workerCount = options.getWorkerPoolSize();
    this.workerExecutor = Executors.newFixedThreadPool(options.getWorkerPoolSize(),
        NamedThreadFactory.create
            ("event-consumer-worker"));
    this.eventQueue = queue;
    this.consumerRepository = consumerRepository;
    this.blockedCheckerMs = options.getBlockedCheckerMs();
    this.checker = BlockedEventChecker.create(this.blockedCheckerMs);
    running = true;
  }

  @Override
  public void start() {
    for (int i = 0; i < workerCount; i++) {
      ConsumerWorker worker = new ConsumerWorker(eventQueue, consumerRepository, checker,
          blockedCheckerMs);
      workerExecutor.submit(worker);
      // TODO shutdown
    }
  }

  @Override
  public void close() {
    //关闭消息订阅
    running = false;
    LOGGER.info("closing consumer, remaining:{}", waitForHandle());
    workerExecutor.shutdown();
  }

  @Override
  public long waitForHandle() {
    return eventQueue.size();
  }

  @Override
  public void consumer(String topic, String resource, EventHandler consumer) {
    HandlerRegistry.instance().register(new HandlerKey(topic, resource), consumer);
  }

  @Override
  public boolean isRunning() {
    return running;
  }

}
