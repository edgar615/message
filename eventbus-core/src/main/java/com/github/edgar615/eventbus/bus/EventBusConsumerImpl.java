package com.github.edgar615.eventbus.bus;

import com.github.edgar615.eventbus.dao.EventConsumerDao;
import com.github.edgar615.eventbus.utils.EventQueue;
import com.github.edgar615.eventbus.utils.NamedThreadFactory;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
public class EventBusConsumerImpl implements EventBusConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventBusConsumer.class);

  private final ExecutorService workerExecutor;

  private final BlockedEventChecker checker;

  private final EventQueue eventQueue;

  private volatile boolean running = false;

  private EventConsumerDao consumerDao;

  private final int workerCount;

  private final long blockedCheckerMs;

  public EventBusConsumerImpl(ConsumerOptions options, EventQueue queue,
      EventConsumerDao consumerDao) {
    this.workerCount = options.getWorkerPoolSize();
    this.workerExecutor = Executors.newFixedThreadPool(options.getWorkerPoolSize(),
        NamedThreadFactory.create
            ("event-consumer-worker"));
    this.eventQueue = queue;
    this.consumerDao = consumerDao;
    this.blockedCheckerMs = options.getBlockedCheckerMs();
    this.checker = BlockedEventChecker.create(this.blockedCheckerMs);
    running = true;
  }

  @Override
  public void start() {
    for (int i = 0; i < workerCount; i++) {
      ConsumerWorker worker = new ConsumerWorker(eventQueue, consumerDao, checker,
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
  public Map<String, Object> metrics() {
    return null;
  }

  @Override
  public long waitForHandle() {
    return eventQueue.size();
  }

  @Override
  public void consumer(String topic, String resource, EventSubscriber handler) {
    SubscriberRegistry.instance().register(new SubscriberKey(topic, resource), handler);
  }

  @Override
  public boolean isRunning() {
    return running;
  }

}
