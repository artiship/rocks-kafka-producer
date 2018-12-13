package com.me.rocks.kafka;

import com.me.rocks.kafka.avro.AvroModel;
import com.me.rocks.kafka.config.RocksThreadFactory;
import com.me.rocks.kafka.delivery.DeliveryStrategyEnum;
import com.me.rocks.kafka.delivery.health.KafkaHealthChecker;
import com.me.rocks.kafka.delivery.health.KafkaHealthCheckerFactory;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategy;
import com.me.rocks.kafka.exception.RocksProducerException;
import com.me.rocks.kafka.jmx.RocksProducerMetric;
import com.me.rocks.kafka.queue.RocksStoreFactory;
import com.me.rocks.kafka.queue.message.AvroKey;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.kafka.queue.serialize.KryoSerializer;
import com.me.rocks.kafka.queue.serialize.Serializer;
import com.me.rocks.queue.QueueItem;
import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.RocksStore;
import com.me.rocks.queue.exception.RocksQueueException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RocksProducer {
    private static final Logger log = LoggerFactory.getLogger(RocksProducer.class);

    private final String topic;
    private final Serializer serializer;
    private final RocksQueue queue;
    private final List<Listener> listeners = new LinkedList<>();
    private final ExecutorService executorService;
    private final DeliveryStrategy strategy;
    private final RocksStore rocksStore;
    private final KafkaHealthChecker kafkaHealthChecker;
    private final RocksProducerMetric rocksProducerMetric;
    private volatile boolean cancelDelivering = false;

    public RocksProducer(final String topic,
                         final Serializer serializer,
                         final Listener listener,
                         final DeliveryStrategy strategy,
                         final RocksStore rocksStore,
                         final KafkaHealthChecker kafkaHealthChecker) {
        this.topic = topic;
        this.serializer = serializer;
        this.strategy = strategy;
        this.rocksStore = rocksStore;
        this.kafkaHealthChecker = kafkaHealthChecker;
        this.rocksProducerMetric = new RocksProducerMetric(this);

        rocksProducerMetric.register();
        this.listeners.add(rocksProducerMetric);

        if(listener != null) {
            this.listeners.add(listener);
        }

        this.queue = rocksStore.createQueue(topic);

        registerShutdownHook();

        executorService = Executors.newSingleThreadExecutor(new RocksThreadFactory("rocks_queue_consumer" + "-" + topic));
        executorService.submit(() -> {
            try {
                final AtomicBoolean lock = new AtomicBoolean(false);
                while (!cancelDelivering) {
                    //guaranty kafka producer sending order
                    if (lock.get()) continue;

                    //waiting for new messages
                    if (queue.isEmpty()) continue;

                    //stop delivery if kafka server is not health
                    if (!isKafkaServerHealth(topic, kafkaHealthChecker))
                        continue;

                    QueueItem consume = queue.consume();

                    //in case dequeue happens faster than enqueue
                    if (consume == null) continue;

                    KVRecord kvRecord = null;
                    try {
                        kvRecord = serializer.deserialize(consume.getValue());
                    } catch (Exception e) {
                        //if consumed message fails in deserialization, just discard it
                        //and notify client
                        queue.removeHead();
                        notifyDeliveryFail(topic, consume, e);
                    }

                    if (kvRecord == null) continue;

                    final String message = kvRecord.toString();
                    notifyBeforeDelivery(topic, message);
                    strategy.delivery(topic, kvRecord, queue, listeners, lock);
                    notifyAfterDelivery(topic, message);
                }
            } catch (Exception e) {
                log.error("delivery messages cause exception ", e);
            }
        });

    }

    private boolean isKafkaServerHealth(final String topic, final KafkaHealthChecker kafkaHealthChecker) {
        return kafkaHealthChecker.isKafkaBrokersAvailable() &&
                kafkaHealthChecker.isKafkaTopicAvailable(topic) &&
                kafkaHealthChecker.isSchemaRegistryAvailable();
    }

    public void send(AvroModel value) throws RocksProducerException {
        this.send(null, value);
    }

    public void send(String key, AvroModel value) throws RocksProducerException {
        AvroKey avroKey = null;

        if(key != null) {
            avroKey = new AvroKey(key);
        }

        synchronized (listeners) {
            listeners.forEach(l -> l.onSend());
        }

        KVRecord kvRecord = new KVRecord(avroKey, value);
        try {
            queue.enqueue(serializer.serialize(kvRecord));
        } catch (RocksQueueException e) {
            throw new RocksProducerException(e);
        }
    }

    public static RocksProducer create(final String topic) {
        return createFast(topic);
    }

    public static RocksProducer createReliable(final String topic) {
        return RocksProducer.builder()
                .topic(topic)
                .kafkaDeliveryStrategy(DeliveryStrategyEnum.RELIABLE)
                .build();
    }

    public static RocksProducer createFast(final String topic) {
        return RocksProducer.builder()
                .topic(topic)
                .kafkaDeliveryStrategy(DeliveryStrategyEnum.FAST)
                .build();
    }

    public String getTopic() {
        return this.topic;
    }

    public boolean isKafkaBrokersAvailable() {
        if(this.kafkaHealthChecker == null) {
            return false;
        }
        return this.kafkaHealthChecker.isKafkaBrokersAvailable();
    }

    public boolean isKafkaTopicAvailable() {
        if(this.kafkaHealthChecker == null) {
            return false;
        }
        return kafkaHealthChecker.isKafkaTopicAvailable(topic);
    }


    public boolean isSchemaRegistryAvailable() {
        if(this.kafkaHealthChecker == null){
            return false;
        }
        return this.kafkaHealthChecker.isSchemaRegistryAvailable();
    }


    public RocksProducerMetric getRocksProducerMetric() {
        return rocksProducerMetric;
    }

    public String getDeliveryMode() {
        return this.strategy.toString();
    }

    public static class Builder {
        private String topic;
        private Serializer serializer;
        private Listener listener;
        private DeliveryStrategy strategy;
        private RocksStore rocksStore;
        private KafkaHealthChecker kafkaHealthChecker;

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

        public Builder rocksStore(RocksStore rocksStore) {
            this.rocksStore = rocksStore;
            return this;
        }

        public Builder kafkaHealthChecker(KafkaHealthChecker kafkaHealthChecker) {
            this.kafkaHealthChecker = kafkaHealthChecker;
            return this;
        }

        public RocksProducer build() {
            Assert.notNull(topic, "Topic must not be null");

            if(serializer == null) {
                serializer = new KryoSerializer();
            }

            if(strategy == null) {
                strategy = DeliveryStrategyEnum.RELIABLE;
            }

            if(rocksStore == null) {
                rocksStore = RocksStoreFactory.INSTANCE.getRocksStore();
            }

            if(kafkaHealthChecker == null) {
                kafkaHealthChecker = KafkaHealthCheckerFactory.INSTANCE.getKafkaHealthChecker();
            }

            return new RocksProducer(topic, serializer, listener, strategy, rocksStore, kafkaHealthChecker);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.debug("Closing application...");

            // Shutdown kafka delivery thread
            this.clear();

            // Free resources allocated by Kafka producer
            strategy.clear();

            // Stop kafka health checker
            kafkaHealthChecker.clear();

            //Close store will close all queue
            rocksStore.close();

            log.info("Application closed.");
        }));
    }

    private void clear() {
        this.cancelDelivering = true;
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
        void onSend();
        void beforeDelivery(String topic, String message);
        void afterDelivery(String topic, String message);
        void onDeliverySuccess(String topic, long offset);
        void onDeliveryFail(String topic, String message, Exception exception);
        void onDeliveryFailDiscard(String topic, String message);
    }

    private static class SimpleListener implements Listener {
        @Override
        public void onDeliveryFail(String topic, String message, Exception exception) {
            log.error("Sending data {} to kafka topic {} failed", message, topic, exception);
        }

        @Override
        public void onDeliveryFailDiscard(String topic, String message) {
            log.debug("Discard kafka topic {} message {}", topic, message);
        }

        @Override
        public void onSend() {

        }

        @Override
        public void beforeDelivery(String topic, String message) {
            log.debug("Before send data {} to kafka topic {}", message, topic);
        }

        @Override
        public void afterDelivery(String topic, String message) {
            log.debug("After send data {} to kafka topic {}", message, topic );
        }

        @Override
        public void onDeliverySuccess(String topic, long offset) {
            log.debug("sending data to kafka topic {} success, offset is {}", topic, offset);
        }
    }

    /**
     * Get all register listeners
     * @return
     */
    public List<Listener> getListeners() {
        return Collections.unmodifiableList(listeners);
    }

    /**
     * Register listener. Note that multiple listeners will be called in order they
     * where registered.
     */
    public void registerListener(Listener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    /**
     * Unregister all listeners of specific type.
     */
    public void unregisterListener(Class<? extends Listener> listenerClass) {
        synchronized (listeners) {
            Iterator<Listener> iterator = listeners.iterator();
            while (iterator.hasNext()) {
                Listener listener = iterator.next();
                if (listenerClass.isInstance(listener)) {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * Unregister a single listener.
     */
    public void unregisterListener(Listener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    private void notifyDeliveryFail(String topic, QueueItem consume, Exception e) {
        synchronized (listeners) {
            listeners.forEach(l -> l.onDeliveryFail(topic, consume.toString(), e));
        }
    }

    private void notifyAfterDelivery(String topic, String message) {
        synchronized (listeners) {
            listeners.forEach(l -> l.afterDelivery(topic, message));
        }
    }

    private void notifyBeforeDelivery(String topic, String message) {
        synchronized (listeners) {
            listeners.forEach(l -> l.beforeDelivery(topic, message));
        }
    }
}
