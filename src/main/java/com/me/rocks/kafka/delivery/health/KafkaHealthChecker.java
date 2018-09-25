package com.me.rocks.kafka.delivery.health;

import com.me.rocks.kafka.config.RocksProducerConfig;
import com.me.rocks.kafka.config.RocksThreadFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaHealthChecker {
    private AtomicBoolean isHealth;
    private ScheduledExecutorService executorService;
    private AdminClient client;

    public KafkaHealthChecker() {
        this.isHealth = new AtomicBoolean(false);
        executorService = Executors.newSingleThreadScheduledExecutor(new RocksThreadFactory("kafka_health_checker"));
        executorService.scheduleAtFixedRate(() -> {
            if(client == null) {
                client = getKafkaAdminClient();
            }
            try {
                ListTopicsResult topics = client.listTopics();
                if(topics == null) {
                    return;
                }
                Set<String> names = topics.names().get();
                if(!names.isEmpty()) {
                    isHealth.set(true);
                }
            } catch (InterruptedException | ExecutionException e) {
                if(client != null) {
                    client.close();
                }
                isHealth.set(false);
            }
        }, 100, 450, TimeUnit.MILLISECONDS);
    }

    public AdminClient getKafkaAdminClient() {
        return AdminClient.create(RocksProducerConfig.getKafkaAdminClientConfig());
    }

    public void clear() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }

            if(client != null) {
                client.close();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    public boolean isKafkaBrokersAlive() {
        return isHealth.get();
    }
}
