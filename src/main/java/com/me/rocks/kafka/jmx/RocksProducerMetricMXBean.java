package com.me.rocks.kafka.jmx;

public interface RocksProducerMetricMXBean {
    String getQueueName();
    long getSendKafkaSucessCount();
    long getSendKafkaFailCount();
    void reset();
}
