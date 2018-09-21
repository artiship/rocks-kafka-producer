package com.me.rocks.kafka.serialize;

import com.me.rocks.kafka.AbstractShould;
import com.me.rocks.kafka.benchmark.model.DifferentUser;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.kafka.queue.serialize.JdkSerializer;
import com.me.rocks.kafka.queue.serialize.KryoSerializer;
import com.me.rocks.kafka.queue.serialize.Serializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class SerializerShould extends AbstractShould {
    private static final Logger log = LoggerFactory.getLogger(SerializerShould.class);

    @Test public void
    should_kryo_serde() {
        execute(new KryoSerializer());
    }

    @Test public void
    should_jdk_serde() {
        execute(new JdkSerializer());
    }

    private void execute(Serializer serializer) {
        byte[] serialize = serializer.serialize(kv);
        KVRecord kvRecord = serializer.deserialize(serialize);

        assertEquals(kvRecord, kv);
        assertEquals(kvRecord.getKey(), kv.getKey());
        DifferentUser differentUser = (DifferentUser) kvRecord.getModel();
        assertEquals(differentUser, user);
    }
}