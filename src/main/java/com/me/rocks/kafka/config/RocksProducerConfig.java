package com.me.rocks.kafka.config;

import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class RocksProducerConfig {

    public static Properties getKafkaAdminClientConfig() {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getConfig(BOOTSTRAP_SERVERS_CONFIG));
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        return properties;
    }

    public static Properties getKafkaProducerConfig() {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getConfig(BOOTSTRAP_SERVERS_CONFIG));
        properties.setProperty(ACKS_CONFIG, ConfigManager.getConfig(ACKS_CONFIG));
        properties.setProperty(RETRIES_CONFIG, ConfigManager.getConfig(RETRIES_CONFIG));
        properties.setProperty(BUFFER_MEMORY_CONFIG, ConfigManager.getConfig(BUFFER_MEMORY_CONFIG));
        properties.setProperty(BATCH_SIZE_CONFIG, ConfigManager.getConfig(BATCH_SIZE_CONFIG));
        properties.setProperty(LINGER_MS_CONFIG, ConfigManager.getConfig(LINGER_MS_CONFIG));

        // for message size exceeds 1M enabling compression
        properties.setProperty(MAX_REQUEST_SIZE_CONFIG, ConfigManager.getConfig(MAX_REQUEST_SIZE_CONFIG));
        properties.setProperty(COMPRESSION_TYPE_CONFIG, ConfigManager.getConfig(COMPRESSION_TYPE_CONFIG));

        // avro part
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, ConfigManager.getConfig(KEY_SERIALIZER_CLASS_CONFIG));
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, ConfigManager.getConfig(VALUE_SERIALIZER_CLASS_CONFIG));
        properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, ConfigManager.getConfig(SCHEMA_REGISTRY_URL_CONFIG));
        return properties;
    }
}
