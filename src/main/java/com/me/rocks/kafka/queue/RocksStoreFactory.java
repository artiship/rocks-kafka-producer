package com.me.rocks.kafka.queue;

import com.me.rocks.kafka.config.RocksQueueConfig;
import com.me.rocks.queue.RocksStore;

public enum RocksStoreFactory {
    INSTANCE;

    private RocksStore rocksStore = new RocksStore(RocksQueueConfig.getRocksStoreOptions());

    public RocksStore getRocksStore() {
        return this.rocksStore;
    }
}
