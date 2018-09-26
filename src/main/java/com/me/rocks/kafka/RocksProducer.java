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

    public RocksProducer(final String topic,
                         final Serializer serializer,
                         final Listener listener,
                         final DeliveryStrategy strategy,
                         final RocksStore rocksStore,
                         final KafkaHealthChecker kafkaHealthChecker,
                         final RocksProducerMetric rocksProducerMetric) {
        this.topic = topic;
        this.serializer = serializer;
        this.strategy = strategy;
        this.rocksStore = rocksStore;
        this.kafkaHealthChecker = kafkaHealthChecker;

        if(listener != null) this.listeners.add(listener);

        rocksProducerMetric.register();
        this.listeners.add(rocksProducerMetric);

        this.queue = rocksStore.createQueue(topic);

        registerShutdownHook();

        executorService = Executors.newSingleThreadExecutor(new RocksThreadFactory("rocks_queue_consumer"));
        executorService.submit(() -> {
            final AtomicBoolean lock = new AtomicBoolean(false);
            while(true) {
                //guaranty kafka producer sending order
                if(lock.get()) continue;

                //waiting for new messages
                if(queue.isEmpty()) continue;

                //stop if kafka brokers are not available
                if(!kafkaHealthChecker.isKafkaBrokersAlive()) continue;

                QueueItem consume = queue.consume();

                //in case dequeue happens faster than enqueue
                if(consume == null) continue;

                KVRecord kvRecord = null;
                try {
                    kvRecord = serializer.deserialize(consume.getValue());
                } catch (Exception e) {
                    //if consumed message fails in deserialization, just discard it
                    //and notify client
                    queue.removeHead();
                    synchronized (listeners) {
                        listeners.forEach(l -> l.onSendFail(topic, consume.toString(), e));
                    }
                }

                if(kvRecord == null) continue;

                final String message = kvRecord.toString();
                synchronized (listeners) {
                    listeners.forEach(l -> l.beforeSend(topic, message));
                }
                strategy.delivery(topic, kvRecord, queue, listeners, lock);
                synchronized (listeners) {
                    listeners.forEach(l -> l.afterSend(topic, message));
                }
            }
        });

    }

    public void send(String key, AvroModel value) throws RocksProducerException {
        KVRecord kvRecord = new KVRecord(key, value);
        try {
            queue.enqueue(serializer.serialize(kvRecord));
        } catch (RocksQueueException e) {
            throw new RocksProducerException(e);
        }
    }

    public static RocksProducer create(final String topic) {
        return createReliable(topic);
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

    public static class Builder {
        private String topic;
        private Serializer serializer;
        private Listener listener;
        private DeliveryStrategy strategy;
        private RocksStore rocksStore;
        private KafkaHealthChecker kafkaHealthChecker;
        private RocksProducerMetric metric;

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

        public Builder metric(RocksProducerMetric metric) {
            this.metric = metric;
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

            if(metric == null) {
                metric = new RocksProducerMetric(topic);
            }

            return new RocksProducer(topic, serializer, listener, strategy, rocksStore, kafkaHealthChecker, metric);
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
        void beforeSend(String topic, String message);
        void afterSend(String topic, String message);
        void onSendSuccess(String topic, long offset);
        void onSendFail(String topic, String message, Exception exception);
    }

    private static class DefaultListener implements Listener {
        @Override
        public void onSendFail(String topic, String message, Exception exception) {
            log.error("Sending data {} to kafka topic {} failed", message, topic, exception);
        }

        @Override
        public void beforeSend(String topic, String message) {
            log.debug("Before send data {} to kafka topic {}", message, topic);
        }

        @Override
        public void afterSend(String topic, String message) {
            log.debug("After send data {} to kafka topic {}", message, topic );
        }

        @Override
        public void onSendSuccess(String topic, long offset) {
            log.debug("sending data to kafka topic {} success, offset is {}", topic, offset);
        }
    }


    public List<Listener> getListeners() {
        return Collections.unmodifiableList(listeners);
    }

    public void registerListener(Listener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

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

    public void unregisterListener(Listener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }
}
