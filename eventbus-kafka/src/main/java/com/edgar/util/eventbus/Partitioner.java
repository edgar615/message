package com.edgar.util.eventbus;

import com.edgar.util.event.Event;

/**
 * Created by Edgar on 2017/4/14.
 *
 * @author Edgar  Date 2017/4/14
 */
public interface Partitioner {

  int partition(Event event);
}
