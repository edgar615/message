package com.edgar.util.eventbus;

/**
 * Created by Edgar on 2017/3/24.
 *
 * @author Edgar  Date 2017/3/24
 */
public class EventbusOptions {

  /**
   * 消费者组，默认值UUID
   */
  private String group;

  /**
   * 消费者ID，默认值UUID
   */
  private String id;


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

}
