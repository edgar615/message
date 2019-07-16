package com.github.edgar615.eventbus.repository;

public enum ConsumeEventState {
  PENDING(1),
  SUCCEED(2),
  FAILED(3),
  EXPIRED(4);

  private final int value;

  ConsumeEventState(int value) {
    this.value = value;
  }

  public int value() {
    return value;
  }
}
