package com.me.rocks.kafka;

import com.me.rocks.kafka.avro.AvroProducerFactory;
import com.me.rocks.kafka.avro.GenericRecordMapper;
import com.me.rocks.kafka.avro.AvroModel;
import com.me.rocks.kafka.message.KVRecord;
import com.me.rocks.kafka.queue.RocksQueueFactory;
import com.me.rocks.kafka.serialize.KryoSerializer;
import com.me.rocks.kafka.serialize.Serializer;
import com.me.rocks.queue.QueueItem;
import com.me.rocks.queue.RocksQueue;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RocksProducer {
    private static final Logger log = LoggerFactory.getLogger(RocksProducer.class);

    private final String topic;
    private final Serializer serializer;
    private final RocksQueue queue;
    private final ExecutorService executorService;
    private final Producer<String, Record> producer;

    public RocksProducer(final String topic) {
        this.topic = topic;
        this.serializer = new KryoSerializer();
        this.queue = RocksQueueFactory.INSTANCE.createQueue(topic);
        this.producer = AvroProducerFactory.INSTANCE.getInstance();

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new RocksQueueConsumer());

        registerShutdownHook();
    }

    public void send(String key, AvroModel value) {
        KVRecord kvRecord = new KVRecord(key, value);
        queue.enqueue(serializer.serialize(kvRecord));
    }

    class RocksQueueConsumer implements Runnable {
        @Override
        public void run() {
            while(true) {
                QueueItem consume = queue.consume();
                AvroModel model = serializer.deserialize(consume.getValue());

                Record record = GenericRecordMapper.mapObjectToRecord(model);

                ProducerRecord<String, Record> producerRecord = new ProducerRecord<>(topic, record);

                producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
                    if (exception == null && metadata.hasOffset()) {
                        log.debug("send data {} to kafka topic {} success, offset is {}",
                                model, topic, metadata.offset());
                    } else {
                        log.error("Send data {} to kafka topic {} failed",
                                model, topic, exception);
                    }

                    queue.dequeue();
                });
            }
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.debug("Closing application...");

            //Close store will close all queue
            RocksQueueFactory.INSTANCE.close();

            // Free resources allocated by Kafka producer
            producer.flush();
            producer.close(2, TimeUnit.SECONDS);

            log.info("Application closed.");
        }));
    }
}
