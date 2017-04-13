package com.edgar.util.eventbus;

import com.edgar.util.eventbus.RecordMeta;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;

/**
 * Created by Edgar on 2017/4/12.
 *
 * @author Edgar  Date 2017/4/12
 */
public class RecordMetaTest {

  @Test
  public void testSort() {
    RecordMeta recordMeta = RecordMeta.create("a", 1, 11834541l, null);
    RecordMeta recordMeta2 = RecordMeta.create("a", 1, 11834538l, null);
    RecordMeta recordMeta3 = RecordMeta.create("a", 1, 11834539l, null);
    RecordMeta recordMeta4 = RecordMeta.create("a", 1, 11834550l, null);
    Set<RecordMeta> set = new TreeSet<>();
    set.add(recordMeta);
    set.add(recordMeta2);
    set.add(recordMeta3);
    set.add(recordMeta4);
    System.out.println(set);
  }
}
