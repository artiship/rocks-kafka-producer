package com.me.rocks.kafka;

import com.me.rocks.kafka.avro.AvroModel;
import com.me.rocks.kafka.avro.GenericRecordMapper;
import com.me.rocks.kafka.delivery.RocksThreadFactory;
import com.me.rocks.kafka.delivery.health.KafkaHealthChecker;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategy;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategyEnum;
import com.me.rocks.kafka.exception.RocksProducerException;
import com.me.rocks.kafka.queue.RocksQueueFactory;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.kafka.queue.serialize.KryoSerializer;
import com.me.rocks.kafka.queue.serialize.Serializer;
import com.me.rocks.queue.QueueItem;
import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.exception.RocksQueueException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RocksProducer {
    private static final Logger log = LoggerFactory.getLogger(RocksProducer.class);

    private final String topic;
    private final Serializer serializer;
    private final RocksQueue queue;
    private final Listener listener;
    private final ExecutorService executorService;

    public RocksProducer(final String topic, final Serializer serializer, final Listener listener, final DeliveryStrategy strategy) {
        this.topic = topic;
        this.serializer = serializer;
        this.listener = listener;

        this.queue = RocksQueueFactory.INSTANCE.createQueue(topic);

        registerShutdownHook(strategy);

        executorService = Executors.newSingleThreadExecutor(new RocksThreadFactory("rocks_queue_consumer"));
        executorService.submit(() -> {
            while(true) {
                if(queue.isEmpty()) {
                    continue;
                }

                if(!KafkaHealthChecker.INSTANCE.isKafkaBrokersAlive()){
                    continue;
                }

                //TODO:: encapsulate queue consume serialize
                QueueItem consume = queue.consume();

                if(consume == null)
                    continue;

                strategy.delivery(getProducerRecord(topic, serializer, consume.getValue()), queue, listener);
            }
        });

    }

    private ProducerRecord<String, Record> getProducerRecord(String topic, Serializer serializer, byte[] value) {
        KVRecord kvRecord = serializer.deserialize(value);

        return new ProducerRecord<>(topic, kvRecord.getKey(),
                GenericRecordMapper.mapObjectToRecord(kvRecord.getModel()));
    }

    public void send(String key, AvroModel value) throws RocksProducerException {
        KVRecord kvRecord = new KVRecord(key, value);
        try {
            queue.enqueue(serializer.serialize(kvRecord));
        } catch (RocksQueueException e) {
            throw new RocksProducerException(e);
        }
    }


    public static class Builder {
        private String topic;
        private Serializer serializer;
        private Listener listener;
        private DeliveryStrategy strategy;

        public Builder() {

        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder serializer(Serializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder listener(Listener listener) {
            this.listener = listener;
            return this;
        }

        public Builder kafkaDeliveryStrategy(DeliveryStrategy strategy) {
            this.strategy = strategy;
            return this;
        }

        public RocksProducer build() {
            Assert.notNull(topic, "Topic must not be null");
            if(serializer == null) {
                serializer = new KryoSerializer();
            }

            if(listener == null) {
                listener = new Listener() {
                    @Override
                    public void beforeSend() {

                    }

                    @Override
                    public void afterSend() {

                    }

                    @Override
                    public void onSendFail(String topic, String message, Exception exception) {
                        log.error("Sending data {} to kafka topic {} failed", message, topic, exception);
                    }

                    @Override
                    public void onSendSuccess(String topic, long offset) {
                        log.debug("sending data to kafka topic {} success, offset is {}", topic, offset);
                    }
                };
            }

            if(strategy == null) {
                strategy = DeliveryStrategyEnum.RELIABLE;
            }

            return new RocksProducer(topic, serializer, listener, strategy);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private void registerShutdownHook(DeliveryStrategy strategy) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.debug("Closing application...");

            //Close store will close all queue
            RocksQueueFactory.INSTANCE.close();

            // Free resources allocated by Kafka producer
            strategy.clear();

            // Stop kafka health checker
            KafkaHealthChecker.INSTANCE.clear();

            // Shutdown kafka delivery thread
            this.clear();

            log.info("Application closed.");
        }));
    }

    private void clear() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    public interface Listener {
        void beforeSend();
        void afterSend();
        void onSendSuccess(String topic, long offset);
        void onSendFail(String topic, String message, Exception exception);
    }
}
