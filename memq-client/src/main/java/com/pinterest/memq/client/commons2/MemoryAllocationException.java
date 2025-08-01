package com.pinterest.memq.client.commons2;

import java.io.IOException;

public class MemoryAllocationException extends Exception {
    public MemoryAllocationException(Throwable e) {
        super(e);
    }
    public MemoryAllocationException(String message) {
    super(message);
  }

    public MemoryAllocationException(String message, Throwable e) {
        super(message, e);
    }
}
