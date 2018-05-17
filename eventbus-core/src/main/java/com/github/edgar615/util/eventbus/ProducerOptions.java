package com.github.edgar615.util.eventbus;

/**
 * Producer的配置属性.
 *
 * @author Edgar  Date 2016/5/17
 */
public class ProducerOptions {

  /**
   * The default number of consumer worker threads to be used  = 2 * number of cores on the machine
   */
  public static final int DEFAULT_WORKER_POOL_SIZE =
          2 * Runtime.getRuntime().availableProcessors();


  private static long DEFAULT_PERIOD = 10 * 1000;

  private static int DEFAULT_MAX_QUOTA = 30000;

  /**
   * 最大配额，当未处理的事件超过配额时，需要拒绝发送
   */
  private long maxQuota = DEFAULT_MAX_QUOTA;

  /**
   * 从存储层查询待发送事件的间隔，单位毫秒
   */
  private long fetchPendingPeriod = DEFAULT_PERIOD;

  /**
   * 工作线程数量
   */
  private int workerPoolSize = DEFAULT_WORKER_POOL_SIZE;

  public ProducerOptions() {

  }

  public long getMaxQuota() {
    return maxQuota;
  }

  /**
   * 设置限流的最大配额，当未处理的事件超过配额时，需要拒绝发送
   *
   * @param maxQuota
   * @return
   */
  public ProducerOptions setMaxQuota(long maxQuota) {
    this.maxQuota = maxQuota;
    return this;
  }

  public long getFetchPendingPeriod() {
    return fetchPendingPeriod;
  }

  public ProducerOptions setFetchPendingPeriod(long fetchPendingPeriod) {
    this.fetchPendingPeriod = fetchPendingPeriod;
    return this;
  }

  public int getWorkerPoolSize() {
    return workerPoolSize;
  }

  /**
   * 设置worker线程池的大小，该线程池主要用户处理事件的业务逻辑
   *
   * @param workerPoolSize 线程池大小.
   * @return ProducerOptions
   */
  public ProducerOptions setWorkerPoolSize(int workerPoolSize) {
    this.workerPoolSize = workerPoolSize;
    return this;
  }

}
