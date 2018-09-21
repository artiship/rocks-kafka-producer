package com.me.rocks.kafka.delivery.health;

public enum KafkaHealthCheckerFactory {
    INSTANCE;

    private final KafkaHealthChecker kafkaHealthChecker = new KafkaHealthChecker();

    public KafkaHealthChecker getKafkaHealthChecker() {
        return this.kafkaHealthChecker;
    }
}
