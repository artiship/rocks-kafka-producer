package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.queue.RocksQueue;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.Producer;

import java.util.concurrent.atomic.AtomicBoolean;

public class DeliveryStrategyFast extends DeliveryStrategyTemplate {
    public DeliveryStrategyFast() {
    }

    public DeliveryStrategyFast(Producer<String, GenericData.Record> producer) {
        super(producer);
    }

    @Override
    public void afterCallback(final RocksQueue queue, final AtomicBoolean lock) {

    }

    @Override
    public void afterSend(final RocksQueue queue, final AtomicBoolean lock) {
        this.removeHead(queue);
        lock.set(false);
    }

}
