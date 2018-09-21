package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.queue.RocksQueue;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public interface DeliveryStrategy {
    void delivery(final ProducerRecord<String, GenericData.Record> producerRecord,
                  final RocksQueue queue,
                  final List<RocksProducer.Listener> listeners);
    void clear();
}
