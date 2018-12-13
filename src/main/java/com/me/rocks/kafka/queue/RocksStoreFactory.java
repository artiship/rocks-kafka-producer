package com.me.rocks.kafka.queue;

import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.kafka.config.RocksQueueConfig;
import com.me.rocks.queue.RocksStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum RocksStoreFactory {
    INSTANCE;

    private RocksStore rocksStore;
    private final Logger log = LoggerFactory.getLogger(RocksProducer.class);

    RocksStoreFactory() {
        try {
            rocksStore = new RocksStore(RocksQueueConfig.getRocksStoreOptions());
        } catch (Exception e) {
            log.error("RocksStore initialize failed", e);
        }
    }

    public RocksStore getRocksStore() {
        return this.rocksStore;
    }
}
