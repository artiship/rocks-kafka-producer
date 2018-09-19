package com.me.rocks.kafka.serialize;

import com.me.rocks.kafka.AbstractShould;
import com.me.rocks.kafka.benchmark.model.DifferentUser;
import com.me.rocks.kafka.message.KVRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class SerializerShould extends AbstractShould {
    private static final Logger log = LoggerFactory.getLogger(SerializerShould.class);
    private long start;
    private long end;

    @Test public void
    should_kryo_serde() {
        execute(new KryoSerializer());
    }

    @Test public void
    should_jdk_serde() {
        execute(new JdkSerializer());
    }

    private void execute(Serializer serializer) {
        String name = serializer.getClass().getName();

        start = System.currentTimeMillis();
        byte[] serialize = serializer.serialize(kv);
        end = System.currentTimeMillis();
        log.info("Use {} serialize cost: {} ms", name, end - start);

        start = System.currentTimeMillis();
        KVRecord kvRecord = serializer.deserialize(serialize);
        end = System.currentTimeMillis();
        log.info("Use {} deserialize cost: {} ms", name, end - start);


        log.info("Use {} serialized size {}",name, serialize.length);

        assertEquals(kvRecord, kv);
        assertEquals(kvRecord.getKey(), kv.getKey());
        DifferentUser differentUser = (DifferentUser) kvRecord.getModel();
        assertEquals(differentUser, user);
    }
}