package com.me.rocks.kafka.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public enum AvroProducerFactory {
    INSTANCE;

    private static final Logger log = LoggerFactory.getLogger(AvroProducerFactory.class);
    private final Producer<String, Record> producer;

    AvroProducerFactory() {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        producer = new KafkaProducer<>(properties);
    }

    public Producer<String, Record> getInstance() {
        return this.producer;
    }
}
