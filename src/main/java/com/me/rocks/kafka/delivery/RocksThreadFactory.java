package com.me.rocks.kafka.delivery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

public class RocksThreadFactory implements ThreadFactory {
    private static final Logger log = LoggerFactory.getLogger(RocksThreadFactory.class);

    private int threadId;
    private String name;

    public RocksThreadFactory(String name) {
        threadId = 1;
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, getThreadName());
        log.info("created new thread with id : " + threadId + " and name : " + t.getName());
        threadId++;
        return t;
    }

    private String getThreadName() {
        return "rocks-kafka-producer-" + name + "-" + threadId;
    }
}
