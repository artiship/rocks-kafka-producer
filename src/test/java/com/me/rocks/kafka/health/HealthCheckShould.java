package com.me.rocks.kafka.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.junit.Test;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class HealthCheckShould {
    @Test public void
    should_health_check() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        try(AdminClient client = AdminClient.create(properties)) {
            ListTopicsResult topics = client.listTopics();
            Set<String> names = topics.names().get();
            if(names.isEmpty()) {

            }
        } catch (InterruptedException | ExecutionException e) {
            // kafka is not available
        }
    }
}
