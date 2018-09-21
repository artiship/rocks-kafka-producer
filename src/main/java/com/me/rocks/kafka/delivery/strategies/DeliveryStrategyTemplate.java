package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.kafka.avro.GenericRecordMapper;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.exception.RocksQueueException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DeliveryStrategyTemplate implements DeliveryStrategy {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Producer<String, Record> producer;

    public DeliveryStrategyTemplate() {
        this.producer = KafkaProducerFactory.INSTANCE.createProducer();
    }

    public DeliveryStrategyTemplate(Producer<String, Record> producer) {
        this.producer = producer;
    }

    protected abstract void afterCallback(RocksQueue queue, AtomicBoolean lock);
    protected abstract void afterSend(RocksQueue queue, AtomicBoolean lock);

    @Override
    public void delivery(final String topic,
                         final KVRecord kvRecord,
                         final RocksQueue queue,
                         final List<RocksProducer.Listener> listeners,
                         final AtomicBoolean lock) {
        try {
            Record record = GenericRecordMapper.mapObjectToRecord(kvRecord.getModel());
            ProducerRecord<String, Record> producerRecord = new ProducerRecord<>(topic, kvRecord.getKey(), record);
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null && metadata.hasOffset()) {
                    synchronized (listeners) {
                        listeners.forEach(listener ->
                                listener.onSendSuccess(topic, metadata.offset()));
                    }
                } else {
                    if(exception instanceof RetriableException) {
                        this.producer.flush();
                    }
                    synchronized (listeners) {
                        listeners.forEach(listener ->
                                listener.onSendFail(topic, kvRecord.toString(), exception));
                    }
                }
                afterCallback(queue, lock);
            });
            afterSend(queue, lock);
        } catch (Exception exception) {
            synchronized (listeners) {
                listeners.forEach(listener ->
                        listener.onSendFail(topic, kvRecord.toString(), exception));
            }
        }
    }

    protected void removeHead(RocksQueue queue) {
        try {
            queue.removeHead();
        } catch (RocksQueueException e) {
            log.error("After kafka send remove head exception", e);
        }
    }

    @Override
    public void clear() {
        producer.flush();
        producer.close();
    }
}
