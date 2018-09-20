package com.me.rocks.kafka.queue;

import com.me.rocks.kafka.AbstractShould;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.kafka.queue.serialize.JdkSerializer;
import com.me.rocks.kafka.queue.serialize.Serializer;
import com.me.rocks.queue.QueueItem;
import com.me.rocks.queue.RocksQueue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class RocksQueueFactoryShould extends AbstractShould {
    private static final Logger log = LoggerFactory.getLogger(RocksQueueFactoryShould.class);

    @Test public void
    should_enqueue_dequeue() {
        Serializer serializer = new JdkSerializer();
        String queuName = "kafk_topic_name";
        RocksQueue queue = RocksQueueFactory.INSTANCE.createQueue(queuName);

        long start = System.currentTimeMillis();
        queue.enqueue(serializer.serialize(kv));
        long end = System.currentTimeMillis();
        log.info("enqueue cost {}",  end - start);
        QueueItem dequeue = queue.dequeue();
        byte[] value = dequeue.getValue();

        KVRecord kvRecord = serializer.deserialize(value);
        assertEquals(kv, kvRecord);
    }

}