package com.github.edgar615.message.bus;

/**
 * Producer的配置属性.预留对象后面有需要可以增加参数
 *
 * @author Edgar  Date 2016/5/17
 */
public class ProducerOptions {

  /**
   * The default number of consumer worker threads to be used  = 2 * number of cores on the machine
   */
  public static final int DEFAULT_WORKER_POOL_SIZE =
          2 * Runtime.getRuntime().availableProcessors();


}
