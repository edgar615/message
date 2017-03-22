package com.edgar.util.eventbus.codec;

import com.edgar.util.eventbus.event.EventAction;

import java.util.Map;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public interface EventActionCodec {

  /**
   * 解码
   *
   * @param map
   * @return
   */
  EventAction decode(Map<String, Object> map);

  /**
   * 编码
   *
   * @param action
   * @return
   */
  Map<String, Object> encode(EventAction action);
}
