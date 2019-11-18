package com.github.edgar615.message.repository;

public enum SendMessageState {
  PENDING(1),
  SUCCEED(2),
  FAILED(3),
  EXPIRED(4);

  private final int value;

  SendMessageState(int value) {
    this.value = value;
  }

  public int value() {
    return value;
  }
}
