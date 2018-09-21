package com.me.rocks.kafka;

import com.me.rocks.kafka.avro.GenericRecordMapper;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategyFast;
import com.me.rocks.kafka.exception.RocksProducerException;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.kafka.queue.serialize.KryoSerializer;
import com.me.rocks.queue.QueueItem;
import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.RocksStore;
import com.me.rocks.queue.StoreOptions;
import com.me.rocks.queue.util.Files;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class RocksProducerShould extends AbstractShould{
    private static final Logger log = LoggerFactory.getLogger(RocksProducerShould.class);

    @Test public void
    should_mock_producer_work() {
        mockProducer.send(recordSent(), (recordMetadata, e) -> log.info("rocks producer is good", e));

        assertEquals(mockProducer.history(), Arrays.asList(recordSent()));
    }

    @Test public void
    should_inject_mock_producer_into_rocks_producer() throws RocksProducerException {
        rocksProducer.send(user.getName(), user);
        rocksProducer.send(user.getName(), user);
        rocksProducer.send(user.getName(), user);

        RocksQueue queue = store.createQueue(topic);
        QueueItem consume = queue.consume();
        KryoSerializer kryoSerializer = new KryoSerializer();
        KVRecord kvRecord = kryoSerializer.deserialize(consume.getValue());

        assertEquals(user, kvRecord.getModel());

        //wait for queue consumer thread send data into kafka
        waitForAwhile();
        assertEquals(mockProducer.history(), Arrays.asList(recordSent(),recordSent(),recordSent()));
    }

    private ProducerRecord<String, Record> recordSent() {
        Record record = GenericRecordMapper.mapObjectToRecord(user);
        return new ProducerRecord<>(topic, user.getName(), record);
    }
}