package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.queue.RocksQueue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public interface DeliveryStrategy {
    void delivery(String topic,
                  KVRecord kvRecord,
                  RocksQueue queue,
                  List<RocksProducer.Listener> listeners,
                  AtomicBoolean lock);

    void clear();
}
