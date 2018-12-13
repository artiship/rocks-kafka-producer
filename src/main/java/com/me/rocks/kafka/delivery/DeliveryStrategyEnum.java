package com.me.rocks.kafka.delivery;

import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategy;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategyFast;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategyReliable;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.queue.RocksQueue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public enum DeliveryStrategyEnum implements DeliveryStrategy {
    /**
     * In this mode, RocksProducer will remove message from rocks queue after invoke kafka producer send, will not
     * waiting for send callback return. This mode will speed up the delivery process.
     */
    FAST {
        private final DeliveryStrategy strategy = new DeliveryStrategyFast();


        @Override
        public void delivery(String topic,
                             KVRecord kvRecord,
                             RocksQueue queue,
                             List<RocksProducer.Listener> listeners,
                             AtomicBoolean lock) {
            this.strategy.delivery(topic, kvRecord, queue, listeners, lock);
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
        public void delivery(String topic,
                             KVRecord kvRecord,
                             RocksQueue queue,
                             List<RocksProducer.Listener> listeners,
                             AtomicBoolean lock) {
            this.strategy.delivery(topic, kvRecord, queue, listeners, lock);
        }

        @Override
        public void clear() {
            this.strategy.clear();
        }
    };
}
