package com.github.edgar615.eventbus.bus;

/**
 * Created by Edgar on 2016/5/17.
 *
 * @author Edgar  Date 2016/5/17
 */
public class ConsumerOptions {

  /**
   * The default number of consumer worker threads to be used  = 2 * number of cores on the machine
   */
  public static final int DEFAULT_WORKER_POOL_SIZE =
          2 * Runtime.getRuntime().availableProcessors();

  private static int DEFAULT_BLOCKER_CHECKER_MS = 1000;

  /**
   * 工作线程数量
   */
  private int workerPoolSize = DEFAULT_WORKER_POOL_SIZE;

  /**
   * 阻塞检查
   */
  private int blockedCheckerMs = DEFAULT_BLOCKER_CHECKER_MS;

  public ConsumerOptions() {

  }

  public int getBlockedCheckerMs() {
    return blockedCheckerMs;
  }

  /**
   * 设置blockedCheckerMs，如果超过blockedCheckerMs仍然未被处理完的事件会打印警告日志.
   *
   * @param blockedCheckerMs 最大阻塞时间
   * @return ConsumerOptions
   */
  public ConsumerOptions setBlockedCheckerMs(int blockedCheckerMs) {
    this.blockedCheckerMs = blockedCheckerMs;
    return this;
  }

  public int getWorkerPoolSize() {
    return workerPoolSize;
  }

  /**
   * 设置worker线程池的大小，该线程池主要用户处理事件的业务逻辑
   *
   * @param workerPoolSize 线程池大小.
   * @return ConsumerOptions
   */
  public ConsumerOptions setWorkerPoolSize(int workerPoolSize) {
    this.workerPoolSize = workerPoolSize;
    return this;
  }
}
