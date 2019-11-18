package com.github.edgar615.message.repository;

public enum ConsumeMessageState {
  PENDING(1),
  SUCCEED(2),
  FAILED(3),
  EXPIRED(4);

  private final int value;

  ConsumeMessageState(int value) {
    this.value = value;
  }

  public int value() {
    return value;
  }
}
