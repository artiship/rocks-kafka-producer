package com.me.rocks.kafka;

import com.me.rocks.kafka.avro.GenericRecordMapper;
import com.me.rocks.kafka.benchmark.model.DifferentUser;
import com.me.rocks.kafka.benchmark.model.UserType;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class kafkaProducerShould {
    private static final Logger log = LoggerFactory.getLogger(kafkaProducerShould.class);

    @Test public void
    send_avro_data() {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        Producer<java.lang.String, Record> producer = new KafkaProducer<>(properties);

        java.lang.String topic = "customer-avro";

        registerShutdownHook(producer);

        DifferentUser user = new DifferentUser();
        user.setFavoriteColor("red");
        user.setFavoriteNumber(10);
        user.setName("samuel");
        user.setUserType(UserType.JUNIOR.toString());

        Record record = GenericRecordMapper.mapObjectToRecord(user);

        ProducerRecord<java.lang.String, Record> producerRecord = new ProducerRecord<>(topic, record);

        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                System.out.println(metadata);
            } else {
                exception.printStackTrace();
            }
        });

        producer.flush();
        producer.close();
    }

    private void registerShutdownHook(Producer<String, Record> producer) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.debug("Closing application...");

                // remember to free resources allocated by Kafka producer
                producer.flush();
                producer.close(2, TimeUnit.SECONDS);

                log.info("Application closed.");
            }
        });
    }
}
