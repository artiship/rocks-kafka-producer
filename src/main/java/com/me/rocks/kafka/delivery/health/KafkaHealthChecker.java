package com.me.rocks.kafka.delivery.health;

import com.me.rocks.kafka.config.RocksProducerConfig;
import com.me.rocks.kafka.config.RocksThreadFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaHealthChecker {
    private static final Logger log = LoggerFactory.getLogger(KafkaHealthChecker.class);

    private final AtomicBoolean isKafkaBrokersAvailable = new AtomicBoolean(false);
    private final AtomicBoolean isSchemaRegistryAvailable = new AtomicBoolean(false);
    private final SchemaRegistryHealthChecker schemaRegistryHealthChecker = new SchemaRegistryHealthChecker();
    private final ScheduledExecutorService executorService =
            Executors.newSingleThreadScheduledExecutor(new RocksThreadFactory("kafka_health_checker"));

    private AdminClient client;
    private Set<String> topics;

    public KafkaHealthChecker() {
        executorService.scheduleAtFixedRate(() -> {
            isKafkaBrokersAvailable.set(checkBrokers());
            isSchemaRegistryAvailable.set(schemaRegistryHealthChecker.isAvailable());

            log.debug("kafka health check result is {}", isKafkaBrokersAvailable.get());
            log.debug("schema registry health check result is {}", isSchemaRegistryAvailable.get());
        }, 100, 5000, TimeUnit.MILLISECONDS);
    }

    private boolean checkBrokers() {
        try {
            if(client == null) client = getKafkaAdminClient();

            ListTopicsResult topics = client.listTopics();
            if(topics == null) return false;

            this.topics = topics.names().get();
            if(this.topics.isEmpty()) return false;
        } catch (Exception e) {
            if(client != null) {
                client.close();
                client = null;
            }

            log.error("Check brokers caught exception", e);
            return false;
        }

        return true;
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
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        if(client != null) {
            client.close();
        }

        if(this.schemaRegistryHealthChecker != null) {
            this.schemaRegistryHealthChecker.clear();
        }
    }

    public boolean isKafkaTopicAvailable(final String topic) {
        if(this.topics == null || topics.isEmpty()) {
            return false;
        }

        return topics.contains(topic);
    }

    public boolean isKafkaBrokersAvailable() {
        return isKafkaBrokersAvailable.get();
    }

    public boolean isSchemaRegistryAvailable() {
        return isSchemaRegistryAvailable.get();
    }
}
