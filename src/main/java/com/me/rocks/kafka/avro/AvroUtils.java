package com.me.rocks.kafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

public class AvroUtils {
    public static String getSchema(AvroModel model) {
        Schema schema = ReflectData.get().getSchema(model.getClass());
        if(schema == null) {
            return null;
        }

        return schema.toString();
    }
}
