package com.me.rocks.kafka.jmx;

public interface RocksProducerMetricMXBean {
    String getTopic();
    String getKafkaDeliveryMode();
    long getKafkaDeliverySuccessCount();
    long getKafkaDeliveryFailCount();
    long getKafkaDeliveryFailDiscardCount();
    double getKafkaDeliverySuccessFifteenMinuteRate();
    boolean isKafkaBrokersAvailable();
    boolean isKafkaTopicAvailable();
    boolean isSchemaRegistryAvailable();
    double getKafkaDeliverySuccessFiveMinuteRate();
    double getKafkaDeliverySuccessOneMinuteRate();
    double getKafkaDeliverySuccessMeanRate();
    double getKafkaDeliveryTime95thPercentile();
    double getRocksProducerSendFifteenMinuteRate();
    double getRocksProducerSendFiveMinuteRate();
    double getRocksProducerSendOneMinuteRate();
    double getRocksProducerSendMeanRate();
    long getRocksProducerSendCount();
    void reset();
}
