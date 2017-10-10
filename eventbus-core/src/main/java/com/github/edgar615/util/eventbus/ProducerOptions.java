package com.github.edgar615.util.eventbus;

/**
 * Producer的配置属性.
 *
 * @author Edgar  Date 2016/5/17
 */
public class ProducerOptions {

  private static long DEFAULT_PERIOD = 5 * 60 * 1000;

  private static int DEFAULT_MAX_QUOTA = 30000;

  /**
   * 最大配额，当未处理的事件超过配额时，需要拒绝发送
   */
  private long maxQuota = DEFAULT_MAX_QUOTA;

  private Metrics metrics = new DummyMetrics();

  /**
   * 从存储层查询待发送事件的间隔，单位毫秒
   */
  private long fetchPendingPeriod = DEFAULT_PERIOD;

  private ProducerStorage producerStorage;

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

  public ProducerStorage getProducerStorage() {
    return producerStorage;
  }

  public ProducerOptions setProducerStorage(ProducerStorage producerStorage) {
    this.producerStorage = producerStorage;
    return this;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public ProducerOptions setMetrics(Metrics metrics) {
    this.metrics = metrics;
    return this;
  }
}
