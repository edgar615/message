package com.github.edgar615.eventbus.kafka;

import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;

/**
 * Created by Edgar on 2017/4/12.
 *
 * @author Edgar  Date 2017/4/12
 */
public class RecordFutureTest {

  @Test
  public void testSort() {
    RecordFuture recordFuture = RecordFuture.create("a", 1, 11834541l, null);
    RecordFuture recordFuture2 = RecordFuture.create("a", 1, 11834538l, null);
    RecordFuture recordFuture3 = RecordFuture.create("a", 1, 11834539l, null);
    RecordFuture recordFuture4 = RecordFuture.create("a", 1, 11834550l, null);
    Set<RecordFuture> set = new TreeSet<>();
    set.add(recordFuture);
    set.add(recordFuture2);
    set.add(recordFuture3);
    set.add(recordFuture4);
    System.out.println(set);
  }
}
