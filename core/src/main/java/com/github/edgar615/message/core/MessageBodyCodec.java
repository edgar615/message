package com.github.edgar615.message.core;

import java.util.Map;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public interface MessageBodyCodec {

  /**
   * 解码
   *
   * @param map
   * @return
   */
  MessageBody decode(Map<String, Object> map);

  /**
   * 编码
   *
   * @param action
   * @return
   */
  Map<String, Object> encode(MessageBody action);

  String name();
}
