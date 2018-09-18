package com.me.rocks.kafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.io.Serializable;

abstract public class AvroModel implements Serializable {
    public String getSchema() {
        Schema schema = ReflectData.get().getSchema(this.getClass());
        if(schema == null) {
            return null;
        }

        return schema.toString();
    }
}
