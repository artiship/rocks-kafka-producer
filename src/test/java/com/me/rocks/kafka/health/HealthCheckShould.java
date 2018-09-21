package com.me.rocks.kafka.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class HealthCheckShould {
    private static final Logger log = LoggerFactory.getLogger(HealthCheckShould.class);

    @Test public void
    should_health_check() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        try(AdminClient client = AdminClient.create(properties)) {
            ListTopicsResult topics = client.listTopics();
            Set<String> names = topics.names().get();
            log.info("got topics {}", names);
            if(names.isEmpty()) {

            }
        } catch (InterruptedException | ExecutionException e) {
            // kafka is not available
        }
    }
}
