package com.me.rocks.kafka.serialize;

import com.me.rocks.kafka.avro.GenericRecordMapper;
import com.me.rocks.kafka.message.KVRecord;
import com.me.rocks.kafka.model.DifferentUser;
import com.me.rocks.kafka.model.UserType;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class KryoSerializerShould {
    private static final Logger log = LoggerFactory.getLogger(KryoSerializerShould.class);

    @Test public void
    should_serialize() {
        KryoSerializer serializer = new KryoSerializer();

        DifferentUser user = new DifferentUser();
        user.setColors(Arrays.asList("red", "black"));
        user.setFavoriteColor("red");
        user.setFavoriteNumber(11);
        user.setName("samuel");
        user.setUserType(UserType.FRESH.toString());

        Record record = GenericRecordMapper.mapObjectToRecord(user);

        KVRecord kv = new KVRecord(user.getName(), record);

        byte[] serialize = serializer.serialize(kv);

        log.info("User serialized size {}", serialize.length);

        Record deserialize = serializer.<Record>deserialize(serialize);

        DifferentUser differentUser = new DifferentUser();
        GenericRecordMapper.mapRecordToObject(deserialize, differentUser);

        assertEquals(user, differentUser);
    }
}