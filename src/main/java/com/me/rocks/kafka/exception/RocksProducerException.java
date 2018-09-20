package com.me.rocks.kafka.exception;

import com.me.rocks.queue.exception.RocksQueueException;

public class RocksProducerException extends Exception {
    public RocksProducerException(RocksQueueException e) {
        super("Rocks producer send cause exception, caused by " + e.getMessage(), e);
    }
}
