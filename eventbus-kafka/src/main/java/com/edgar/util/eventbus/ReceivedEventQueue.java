package com.edgar.util.eventbus;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.edgar.util.event.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by Edgar on 2017/3/22.
 *
 * @author Edgar  Date 2017/3/22
 */
public class ReceivedEventQueue {

  private ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
  private ListeningExecutorService executorService =
          MoreExecutors.listeningDecorator(executor);

  private final Runnable runnable;

  private LinkedList<ConsumerRecord<String, Event>> events = new LinkedList<>();

  private List<Long> sets = new CopyOnWriteArrayList<>();

  private boolean running = false;


  public ReceivedEventQueue() {
    this.runnable = () -> {
      for (; ; ) {
        final ConsumerRecord<String, Event> event;
        synchronized (events) {
          event = events.poll();
          if (event == null) {
            running = false;
            return;
          }
        }

        try {
          handle(event);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
  }


  public void handle(ConsumerRecord<String, Event> event) throws InterruptedException {
    System.out.println("sets" + sets);
    ListenableFuture f = executorService.submit(() -> {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
//          System.out.println(System.currentTimeMillis() + ":" + event);
      } catch (Throwable t) {
        t.printStackTrace();
//          log.error("Caught unexpected Throwable", t);
      }
    });
    Futures.addCallback(f, new FutureCallback() {
      @Override
      public void onSuccess(Object o) {
//          sets.add(event.offset());
        sets.add(event.offset());
        System.out.println("getCorePoolSize:" + executor.getCorePoolSize());
        System.out.println("getActiveCount:" + executor.getActiveCount());
        System.out.println("getCompletedTaskCount:" + executor.getCompletedTaskCount());
        System.out.println("getLargestPoolSize:" + executor.getLargestPoolSize());
        System.out.println("getPoolSize:" + executor.getPoolSize());
        System.out.println("getTaskCount:" + executor.getTaskCount());
//        System.out.println("getQueue:" + executor.getQueue().size());
      }

      @Override
      public void onFailure(Throwable throwable) {

      }
    });
  }

  public void execute(ConsumerRecord<String, Event> event) {
    synchronized (events) {
      events.add(event);
      if (!running) {
        running = true;
        runnable.run();
      }
    }
  }

  public boolean running() {
    synchronized (events) {
      return running;
    }
  }

  public List<Long> getSets() {
    return sets;
  }

  public void setSets(List<Long> sets) {
    this.sets = sets;
  }
}
