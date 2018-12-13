package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.kafka.config.RocksProducerConfig;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public enum KafkaProducerFactory {
    INSTANCE;

    private final Producer<Record, Record> producer =
            new KafkaProducer<>(RocksProducerConfig.getKafkaProducerConfig());


    public Producer<Record, Record> createProducer() {
        return this.producer;
    }
}
