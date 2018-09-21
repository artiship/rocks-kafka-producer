package com.me.rocks.kafka.jmx;

public interface RocksProducerMetricMXBean {
    long getSendKafkaSucessCount();
    long getSendKafkaFailCount();
    void reset();
}
