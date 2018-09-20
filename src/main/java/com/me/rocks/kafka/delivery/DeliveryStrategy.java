package com.me.rocks.kafka.delivery;

import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.queue.RocksQueue;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface DeliveryStrategy {
    void delivery(final ProducerRecord<String, GenericData.Record> producerRecord,
                         final RocksQueue queue,
                         final RocksProducer.Listener listener);
}
