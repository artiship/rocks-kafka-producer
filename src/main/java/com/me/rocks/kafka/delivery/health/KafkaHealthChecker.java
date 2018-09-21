package com.me.rocks.kafka.delivery.health;

import com.me.rocks.kafka.config.RocksThreadFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public enum KafkaHealthChecker {
    INSTANCE;

    private AtomicBoolean isHealth;
    private ScheduledExecutorService executorService;

    KafkaHealthChecker() {
        this.isHealth = new AtomicBoolean(false);
        executorService = Executors.newSingleThreadScheduledExecutor(new RocksThreadFactory("kafka_health_checker"));
        executorService.scheduleAtFixedRate(() -> {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put("connections.max.idle.ms", 10000);
            properties.put("request.timeout.ms", 5000);
            try(AdminClient client = AdminClient.create(properties)) {
                ListTopicsResult topics = client.listTopics();
                if(topics == null) {
                    return;
                }
                Set<String> names = topics.names().get();
                if(!names.isEmpty()) {
                    isHealth.set(true);
                }
            } catch (InterruptedException | ExecutionException e) {
                isHealth.set(false);
            }
        }, 100, 450, TimeUnit.MILLISECONDS);
    }

    public boolean isKafkaBrokersAlive() {
        return isHealth.get();
    }

    public void clear() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }
}
