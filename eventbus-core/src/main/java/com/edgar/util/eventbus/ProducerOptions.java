package com.edgar.util.eventbus;

/**
 * Producer的配置属性.
 *
 * @author Edgar  Date 2016/5/17
 */
public class ProducerOptions {

  private static long DEFAULT_PERIOD = 5 * 60 * 1000;

  private Metrics metrics = new DummyMetrics();

  /**
   * 从存储层查询待发送事件的间隔，单位毫秒
   */
  private long fetchPendingPeriod = DEFAULT_PERIOD;

  private ProducerStorage producerStorage;

  public ProducerOptions() {

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
