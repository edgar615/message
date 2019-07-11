package com.github.edgar615.eventbus.bus;

import java.util.concurrent.ScheduledExecutorService;

public interface EventBusScheduler {

  void start();

  void close();

  ScheduledExecutorService executor();
}
