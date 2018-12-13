package com.me.rocks.kafka;

import com.me.rocks.kafka.avro.GenericRecordMapper;
import com.me.rocks.kafka.benchmark.model.DifferentUser;
import com.me.rocks.kafka.exception.RocksProducerException;
import com.me.rocks.kafka.queue.message.AvroKey;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.kafka.queue.serialize.KryoSerializer;
import com.me.rocks.queue.QueueItem;
import com.me.rocks.queue.RocksQueue;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class RocksProducerShould extends AbstractShould{
    private static final Logger log = LoggerFactory.getLogger(RocksProducerShould.class);

    @Test public void
    should_mock_producer_work() {
        mockProducer.send(recordSent(user), (recordMetadata, e) -> log.info("rocks producer is good", e));

        assertEquals(mockProducer.history(), Arrays.asList(recordSent(user)));
    }

    @Test public void
    should_inject_mock_producer_into_rocks_producer() throws RocksProducerException {
        DifferentUser user1 = DifferentUser.mock();
        DifferentUser user2 = DifferentUser.mock();
        DifferentUser user3 = DifferentUser.mock();

        user1.setName("user1");
        user2.setName("user2");
        user3.setName("user3");

        rocksProducer.send(user1.getName(), user1);
        rocksProducer.send(user2.getName(), user2);
        rocksProducer.send(user3.getName(), user3);

        RocksQueue queue = store.createQueue(topic);
        QueueItem consume = queue.consume();
        KryoSerializer kryoSerializer = new KryoSerializer();
        KVRecord kvRecord = kryoSerializer.deserialize(consume.getValue());

        assertEquals(user1, kvRecord.getValue());

        //wait for queue consumer thread send data into kafka
        waitForAwhile();
        assertEquals(mockProducer.history(), Arrays.asList(recordSent(user1),recordSent(user2),recordSent(user3)));
    }

    private ProducerRecord<Record, Record> recordSent(DifferentUser user) {
        Record key = GenericRecordMapper.mapObjectToRecord(new AvroKey(user.getName()));
        Record value = GenericRecordMapper.mapObjectToRecord(user);
        return new ProducerRecord<>(topic, key, value);
    }
}