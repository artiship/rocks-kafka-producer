package com.me.rocks.kafka.queue;

import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.RocksStore;
import com.me.rocks.queue.StoreOptions;

public enum RocksQueueFactory {
    INSTANCE;

    private static final String ROCKSDB_NAME = "rocks_db";
    private final RocksStore rocksStore;

    RocksQueueFactory() {
        StoreOptions options = new StoreOptions.Builder().setDatabase(ROCKSDB_NAME).build();
        options.setDefaults();
        rocksStore = new RocksStore(options);
    }

    public RocksQueue createQueue(String queuName) {
        return rocksStore.createQueue(queuName);
    }

    public void close() {
        if(this.rocksStore != null) {
            this.rocksStore.close();
        }
    }
}
