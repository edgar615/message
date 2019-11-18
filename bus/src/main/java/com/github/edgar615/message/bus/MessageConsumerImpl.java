package com.github.edgar615.message.bus;

import com.github.edgar615.message.repository.MessageConsumerRepository;
import com.github.edgar615.message.utils.MessageQueue;
import com.github.edgar615.message.utils.NamedThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Edgar on 2017/4/18.
 *
 * @author Edgar  Date 2017/4/18
 */
class MessageConsumerImpl implements MessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

  private final ExecutorService workerExecutor;

  private final BlockedMessageChecker checker;

  private final MessageQueue messageQueue;

  private volatile boolean running = false;

  private MessageConsumerRepository consumerRepository;

  private final int workerCount;

  private final long blockedCheckerMs;

  MessageConsumerImpl(ConsumerOptions options, MessageQueue queue,
      MessageConsumerRepository consumerRepository) {
    this.workerCount = options.getWorkerPoolSize();
    this.workerExecutor = Executors.newFixedThreadPool(options.getWorkerPoolSize(),
        NamedThreadFactory.create
            ("core-consumer-worker"));
    this.messageQueue = queue;
    this.consumerRepository = consumerRepository;
    this.blockedCheckerMs = options.getBlockedCheckerMs();
    this.checker = BlockedMessageChecker.create(this.blockedCheckerMs);
    running = true;
  }

  @Override
  public void start() {
    for (int i = 0; i < workerCount; i++) {
      ConsumerWorker worker = new ConsumerWorker(messageQueue, consumerRepository, checker,
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
    checker.close();
  }

  @Override
  public long waitForHandle() {
    return messageQueue.size();
  }

  @Override
  public void consumer(String topic, String resource, MessageHandler consumer) {
    HandlerRegistry.instance().register(new HandlerKey(topic, resource), consumer);
  }

  @Override
  public boolean isRunning() {
    return running;
  }

}
