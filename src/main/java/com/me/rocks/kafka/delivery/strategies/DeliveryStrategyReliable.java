package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.queue.RocksQueue;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.Producer;

import java.util.concurrent.atomic.AtomicBoolean;

public class DeliveryStrategyReliable extends DeliveryStrategyTemplate {

    public DeliveryStrategyReliable() {
    }

    public DeliveryStrategyReliable(Producer<Record, Record> producer) {
        super(producer);
    }

    @Override
    public void onCallbackSuccess(final RocksQueue queue, AtomicBoolean lock) {
        this.removeHead(queue);
        lock.set(false);
    }

    @Override
    public void afterKafkaSend(final RocksQueue queue, AtomicBoolean lock) {
        this.producer.flush(); //because reliable mode is a sync mode, should flush every time after send.
    }

}
