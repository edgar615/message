package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Message;

public interface MessageHandler {

  void handle(Message message);

}
