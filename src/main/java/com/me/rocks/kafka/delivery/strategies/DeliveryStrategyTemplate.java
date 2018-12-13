package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.kafka.RocksProducer.Listener;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.exception.RocksQueueException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.me.rocks.kafka.avro.GenericRecordMapper.mapObjectToRecord;

public abstract class DeliveryStrategyTemplate implements DeliveryStrategy {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final Producer<Record, Record> producer;

    public DeliveryStrategyTemplate() {
        this.producer = KafkaProducerFactory.INSTANCE.createProducer();
    }

    public DeliveryStrategyTemplate(Producer<Record, Record> producer) {
        this.producer = producer;
    }

    protected abstract void onCallbackSuccess(RocksQueue queue, AtomicBoolean lock);
    protected abstract void afterKafkaSend(RocksQueue queue, AtomicBoolean lock);

    @Override
    public void delivery(final String topic,
                         final KVRecord kvRecord,
                         final RocksQueue queue,
                         final List<Listener> listeners,
                         final AtomicBoolean lock) {
        try {
            lock.set(true);

            Record key = kvRecord.getKey() == null ? null: mapObjectToRecord(kvRecord.getKey());
            Record value = kvRecord.getValue() == null ? null: mapObjectToRecord(kvRecord.getValue());
            ProducerRecord<Record, Record> producerRecord = new ProducerRecord<>(topic, key, value);
            producer.send(producerRecord, (metadata, exception) -> {
                try {
                    if (exception == null && metadata.hasOffset()) {
                        onCallbackSuccess(queue, lock);
                        notifyDeliverySuccess(topic, listeners, metadata);
                    } else {
                        whenDeliveryWithException(topic, kvRecord, queue, listeners, lock, exception);
                    }
                } catch (Exception e) {
                    whenDeliveryWithException(topic, kvRecord, queue, listeners, lock, exception);
                }
            });
            afterKafkaSend(queue, lock);
        } catch (Exception exception) {
            whenDeliveryWithException(topic, kvRecord, queue, listeners, lock, exception);
        }
    }

    private void whenDeliveryWithException(final String topic,
                                           final KVRecord kvRecord,
                                           final RocksQueue queue,
                                           final List<Listener> listeners,
                                           final AtomicBoolean lock,
                                           final Exception exception) {
        notifyDeliveryFail(topic, kvRecord, listeners, exception);
        if(shouldDiscard(queue, lock, exception)) {
            this.removeHead(queue);
            notifyDeliveryFailDiscard(topic, kvRecord, listeners);
        }
        log.error("Deliver message to kafka got exception", exception);
    }

    private boolean shouldDiscard(final RocksQueue queue,
                                  final AtomicBoolean lock,
                                  final Exception exception) {
        lock.set(false);

        if(exception instanceof TimeoutException) {
            log.warn("Encounter timeout exception",exception);
            return false;
        }

        return true;
    }

    protected void removeHead(RocksQueue queue) {
        try {
            queue.removeHead();
        } catch (RocksQueueException e) {
            log.error("After kafka send remove head exception", e);
        }
    }

    private void notifyDeliverySuccess(final String topic,
                                       final List<Listener> listeners,
                                       final RecordMetadata metadata) {
        synchronized (listeners) {
            listeners.forEach(listener ->
                    listener.onDeliverySuccess(topic, metadata.offset()));
        }
    }

    private void notifyDeliveryFailDiscard(final String topic,
                                           final KVRecord kvRecord,
                                           final List<Listener> listeners) {
        synchronized (listeners) {
            listeners.forEach(listener ->
                    listener.onDeliveryFailDiscard(topic, kvRecord.toString()));
        }
    }

    private void notifyDeliveryFail(final String topic,
                                    final KVRecord kvRecord,
                                    final List<Listener> listeners,
                                    final Exception exception) {
        synchronized (listeners) {
            listeners.forEach(listener ->
                    listener.onDeliveryFail(topic, kvRecord.toString(), exception));
        }
    }

    @Override
    public void clear() {
        producer.flush();
        producer.close();
    }
}
