package com.me.rocks.kafka.delivery;

import com.me.rocks.kafka.RocksProducer.Listener;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategy;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategyFast;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategyReliable;
import com.me.rocks.queue.RocksQueue;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

import static org.apache.avro.generic.GenericData.Record;

public enum DeliveryStrategyEnum implements DeliveryStrategy {
    /**
     * In this mode, RocksProducer will remove message from rocks queue after invoke kafka producer send, will not
     * waiting for send callback return. This mode will speed up the delivery process.
     */
    FAST {
        private final DeliveryStrategy strategy = new DeliveryStrategyFast();
        @Override
        public void delivery(ProducerRecord<String, Record> producerRecord, RocksQueue queue, List<Listener> listeners) {
            this.strategy.delivery(producerRecord, queue, listeners);
        }

        @Override
        public void clear() {
            this.strategy.clear();
        }
    },
    /**
     * In this mode, RocksProducer will not remove message from rocks queue after consume, until the kafka
     * send callback return and infer that the message has been written into kafka broker successfully.
     */
    RELIABLE {
        private final DeliveryStrategy strategy = new DeliveryStrategyReliable();

        @Override
        public void delivery(ProducerRecord<String, Record> producerRecord, RocksQueue queue, List<Listener> listeners) {
            this.strategy.delivery(producerRecord, queue, listeners);
        }

        @Override
        public void clear() {
            this.strategy.clear();
        }
    };
}
