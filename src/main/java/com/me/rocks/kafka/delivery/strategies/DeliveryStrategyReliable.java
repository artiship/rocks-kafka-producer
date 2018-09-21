package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.queue.RocksQueue;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.Producer;

import java.util.concurrent.atomic.AtomicBoolean;

public class DeliveryStrategyReliable extends DeliveryStrategyTemplate {

    public DeliveryStrategyReliable() {
    }

    public DeliveryStrategyReliable(Producer<String, GenericData.Record> producer) {
        super(producer);
    }

    @Override
    public void afterCallback(final RocksQueue queue, AtomicBoolean lock) {
        this.removeHead(queue);
        lock.set(false);
    }

    @Override
    public void afterSend(final RocksQueue queue, AtomicBoolean lock) {

    }

}
