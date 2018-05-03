package com.github.edgar615.util.eventbus;

import com.github.edgar615.util.event.Event;

import java.util.function.Function;

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

  private static int DEFAULT_MAX_QUOTA = 30000;

  /**
   * 工作线程数量
   */
  private int workerPoolSize = DEFAULT_WORKER_POOL_SIZE;

  /**
   * 阻塞检查
   */
  private int blockedCheckerMs = DEFAULT_BLOCKER_CHECKER_MS;

  /**
   * 最大配额，当未处理的事件超过配额时，需要暂停消费
   */
  private int maxQuota = DEFAULT_MAX_QUOTA;

  private Function<Event, Boolean> blackListFilter = null;

  private Function<Event, String> identificationExtractor = null;

  private Metrics metrics = new DummyMetrics();

  public ConsumerOptions() {

  }

  public int getMaxQuota() {
    return maxQuota;
  }

  /**
   * 设置限流的最大配额，当未处理的事件超过配额时，需要暂停消费
   *
   * @param maxQuota
   * @return
   */
  public ConsumerOptions setMaxQuota(int maxQuota) {
    this.maxQuota = maxQuota;
    return this;
  }

  public Function<Event, Boolean> getBlackListFilter() {
    return blackListFilter;
  }

  /**
   * 设置黑名单的过滤器，如果filter返回true，表示不处理这个事件
   *
   * @param filter
   * @return
   */
  public ConsumerOptions setBlackListFilter(
          Function<Event, Boolean> filter) {
    this.blackListFilter = filter;
    return this;
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

  public Metrics getMetrics() {
    return metrics;
  }

  public ConsumerOptions setMetrics(Metrics metrics) {
    this.metrics = metrics;
    return this;
  }

  public Function<Event, String> getIdentificationExtractor() {
    return identificationExtractor;
  }

  /**
   * 根据某个标识符，将事件按顺序处理
   *
   * @param identificationExtractor
   * @return ConsumerOptions
   */
  public ConsumerOptions setIdentificationExtractor(
          Function<Event, String> identificationExtractor) {
    this.identificationExtractor = identificationExtractor;
    return this;
  }
}
