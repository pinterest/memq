package com.pinterest.memq.client.consumer;

import java.util.Map;

public interface OffsetCommitCallback {
  void onCompletion(Map<Integer, Long> offsets, Exception exception);
}
