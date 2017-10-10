package com.github.edgar615.util.eventbus;

/**
 * Created by Edgar on 2017/3/24.
 *
 * @author Edgar  Date 2017/3/24
 */
public interface Callback<T> {

  void onCallBack(EventFuture<T> future);

}
