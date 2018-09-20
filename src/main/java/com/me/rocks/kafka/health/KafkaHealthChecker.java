package com.me.rocks.kafka.health;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public enum KafkaHealthChecker {
    INSTANCE;

    private AtomicBoolean isHealth;
    private AdminClient client;

    KafkaHealthChecker() {
        this.isHealth = new AtomicBoolean(false);
        this.client = AdminClient.create(new Properties());
    }
}
