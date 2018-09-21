package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.kafka.delivery.config.KafkaProducerConfig;
import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.exception.RocksQueueException;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * In this mode, RocksProducer will not remove message from rocks queue after consume, until the kafka
 * send callback return and infer that the message has been written into kafka broker successfully.
 */
public enum DeliveryStrategyEnum implements DeliveryStrategy {
    FAST{
        private KafkaProducer<String, GenericData.Record> producer = new KafkaProducer<String, GenericData.Record>(getProducerProperties());

        @Override
        public void delivery(final ProducerRecord<String, GenericData.Record> producerRecord,
                             final RocksQueue queue,
                             final RocksProducer.Listener listener) {
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null && metadata.hasOffset()) {
                    listener.onSendSuccess(producerRecord.topic(), metadata.offset());
                } else {
                    listener.onSendFail(producerRecord.topic(),producerRecord.value().toString(), exception);
                }
            });

            try {
                queue.removeHead();
            } catch (RocksQueueException e) {
                log.error("After kafka send remove head exception", e);
            }
        }

        @Override
        public Properties getProducerProperties() {
            return KafkaProducerConfig.loadProperties();
        }

        @Override
        public void clear() {
            producer.flush();
            producer.close();
        }
    },
    RELIABLE {
        private KafkaProducer<String, GenericData.Record> producer = new KafkaProducer<String, GenericData.Record>(getProducerProperties());

        @Override
        public void delivery(final ProducerRecord<String, GenericData.Record> producerRecord,
                             final RocksQueue queue,
                             final RocksProducer.Listener listener) {
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null && metadata.hasOffset()) {
                    listener.onSendSuccess(producerRecord.topic(), metadata.offset());
                } else {
                    listener.onSendFail(producerRecord.topic(),producerRecord.value().toString(), exception);
                }

                try {
                    queue.removeHead();
                } catch (RocksQueueException e) {
                    log.error("After kafka send remove head exception", e);
                }
            });
        }

        @Override
        public Properties getProducerProperties() {
            return KafkaProducerConfig.loadProperties();
        }

        @Override
        public void clear() {
            producer.flush();
            producer.close();
        }
    };

    private static final Logger log = LoggerFactory.getLogger(DeliveryStrategyEnum.class);
    public abstract Properties getProducerProperties();
}
