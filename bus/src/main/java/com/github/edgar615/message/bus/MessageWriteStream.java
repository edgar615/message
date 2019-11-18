package com.github.edgar615.message.bus;

import com.github.edgar615.message.core.Message;
import java.util.concurrent.CompletableFuture;

public interface MessageWriteStream {

  CompletableFuture<Message> send(Message message);

  void start();

  void close();
}
