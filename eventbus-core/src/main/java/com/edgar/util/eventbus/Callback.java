package com.edgar.util.eventbus;

import com.edgar.util.eventbus.event.Event;

/**
 * Created by Edgar on 2017/3/24.
 *
 * @author Edgar  Date 2017/3/24
 */
public interface Callback {

  void onCallBack(EventFuture future);

}
