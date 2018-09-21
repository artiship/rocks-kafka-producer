package com.me.rocks.kafka.config;

import com.me.rocks.queue.StoreOptions;

public class RocksQueueConfig {

    private static final String ROCKSDB_LOCATION_DIRECTORY_CONFIG = "rocksdb.location.directory";
    private static final String ROCKSDB_DATABASE_NAME_CONFIG = "rocksdb.database.name";
    private static final String ROCKSDB_MEMORY_SIZE_CONFIG = "rocksdb.memeory.size";
    private static final String ROCKSDB_FILE_SIZE_BASE_CONFIG = "rocksdb.file.size.base";
    private static final String ROCKSDB_WRITE_BUFFER_SIZE_CONFIG = "rocksdb.write.buffer.size";
    private static final String ROCKSDB_WRITE_BUFFER_NUMBER_CONFIG = "rocksdb.write.buffer.number";
    private static final String ROCKSDB_PARALLEL_NUMBER_CONFIG = "rocksdb.parallel.number";

    public static StoreOptions getRocksStoreOptions() {
        return StoreOptions.builder()
                .directory(ConfigManager.getConfig(ROCKSDB_LOCATION_DIRECTORY_CONFIG))
                .database(ConfigManager.getConfig(ROCKSDB_DATABASE_NAME_CONFIG))
                .memorySize(Integer.valueOf(ConfigManager.getConfig(ROCKSDB_MEMORY_SIZE_CONFIG)))
                .fileSizeBase(Integer.valueOf(ConfigManager.getConfig(ROCKSDB_FILE_SIZE_BASE_CONFIG)))
                .writeBufferNumber(Integer.valueOf(ConfigManager.getConfig(ROCKSDB_WRITE_BUFFER_NUMBER_CONFIG)))
                .writeBufferSize(Integer.valueOf(ConfigManager.getConfig(ROCKSDB_WRITE_BUFFER_SIZE_CONFIG)))
                .parallel(Integer.valueOf(ConfigManager.getConfig(ROCKSDB_PARALLEL_NUMBER_CONFIG)))
                .build();
    }
}
