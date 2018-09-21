package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.exception.RocksQueueException;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public abstract class DeliveryStrategyTemplate implements DeliveryStrategy {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    private final KafkaProducer<String, GenericData.Record> producer = new KafkaProducer<>(getProducerProperties());

    public abstract void onCallBack(final RocksQueue queue);
    public abstract void afterSend(final RocksQueue queue);
    public abstract Properties getProducerProperties();

    public void delivery(final ProducerRecord<String, GenericData.Record> producerRecord,
                         final RocksQueue queue,
                         final List<RocksProducer.Listener> listeners) {
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null && metadata.hasOffset()) {
                synchronized (listeners) {
                    listeners.forEach(listener ->
                            listener.onSendSuccess(producerRecord.topic(), metadata.offset()));
                }
            } else {
                synchronized (listeners) {
                    listeners.forEach(listener ->
                            listener.onSendFail(producerRecord.topic(), producerRecord.value().toString(), exception));
                }
            }
            onCallBack(queue);
        });
        afterSend(queue);
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
