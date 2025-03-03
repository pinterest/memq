package com.pinterest.memq.client.producer2;

public class BufferedRequestDispatcher implements Runnable {

    private final BufferedRequestManager.MemoryBoundRequestBuffer requestBuffer;

    protected BufferedRequestDispatcher(BufferedRequestManager.MemoryBoundRequestBuffer requestBuffer) {
        this.requestBuffer = requestBuffer;
    }

    @Override
    public void run() {
        while (true) {
            requestBuffer
        }
    }
}
