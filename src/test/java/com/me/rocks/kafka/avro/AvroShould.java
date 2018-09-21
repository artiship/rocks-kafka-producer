package com.me.rocks.kafka.avro;

import com.me.rocks.kafka.benchmark.model.DifferentUser;
import com.me.rocks.kafka.benchmark.model.UserType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AvroShould {
    private static final Logger log = LoggerFactory.getLogger(AvroShould.class);
    private Schema schema = ReflectData.get().getSchema(DifferentUser.class);

    @Test public void
    should_generate_avro_schema_from_model() {
        assertEquals(Arrays.asList(Schema.create(NULL), Schema.create(STRING)),
                schema.getField("name").schema().getTypes());
        assertNull(schema.getField("ignore"));
        assertEquals(0, schema.getField("favoriteNumber").defaultVal());
        log.info(schema.toString());
    }

    @Test public void
    should_kafka_send() {
        GenericData.Record avroRecord = new GenericData.Record(schema);
        avroRecord.put("name", "samuel");
        avroRecord.put("favoriteColor", "red");
        avroRecord.put("userType", UserType.FRESH);
        avroRecord.put("colors", Arrays.asList("yellow", "pink", "dark black"));
    }

    @Test public void
    should_bean_utils_copy_bean_to_avro_record() {
        DifferentUser user = new DifferentUser();
        user.setFavoriteColor("red");
        user.setFavoriteNumber(10);
        user.setName("samuel");
        user.setUserType(UserType.JUNIOR.toString());

        GenericData.Record record = GenericRecordMapper.mapObjectToRecord(user);

    }
}


