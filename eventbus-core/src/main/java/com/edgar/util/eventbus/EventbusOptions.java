package com.edgar.util.eventbus;

/**
 * Created by Edgar on 2017/3/24.
 *
 * @author Edgar  Date 2017/3/24
 */
public class EventbusOptions {

  private static long DEFAULT_PERIOD = 5 * 60 * 1000;

  private static int DEFAULT_SEND_MAXSIZE = 10000;

  /**
   * 消费者组，默认值UUID
   */
  private String group;

  /**
   * 消费者ID，默认值UUID
   */
  private String id;

  /**
   * 从存储层查询待发送事件的间隔，单位毫秒
   */
  private long fetchPendingPeriod = DEFAULT_PERIOD;

  private SendBackend sendBackend;

  private SendStorage sendStorage;

  private int maxSendSize = DEFAULT_SEND_MAXSIZE;


  public String getGroup() {
    return group;
  }

  /**
   * 设置eventbus所属的消费者组，同一个分区下，同一个组的两个消费者只有一个能够读取消息.
   *
   * @param group 组名
   * @return KafkaEventbusOptions
   */
  public EventbusOptions setGroup(String group) {
    this.group = group;
    return this;
  }

  public String getId() {
    return id;
  }

  /**
   * eventbus的ID.
   *
   * @param id id
   * @return KafkaEventbusOptions
   */
  public EventbusOptions setId(String id) {
    this.id = id;
    return this;
  }

  public long getFetchPendingPeriod() {
    return fetchPendingPeriod;
  }

  public EventbusOptions setFetchPendingPeriod(long fetchPendingPeriod) {
    this.fetchPendingPeriod = fetchPendingPeriod;
    return this;
  }

  public int getMaxSendSize() {
    return maxSendSize;
  }

  public EventbusOptions setMaxSendSize(int maxSendSize) {
    this.maxSendSize = maxSendSize;
    return this;
  }

  public SendBackend getSendBackend() {
    return sendBackend;
  }

  public EventbusOptions setSendBackend(SendBackend sendBackend) {
    this.sendBackend = sendBackend;
    return this;
  }

  public SendStorage getSendStorage() {
    return sendStorage;
  }

  public EventbusOptions setSendStorage(SendStorage sendStorage) {
    this.sendStorage = sendStorage;
    return this;
  }
}
