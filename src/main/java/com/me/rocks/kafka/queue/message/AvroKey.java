package com.me.rocks.kafka.queue.message;

import com.me.rocks.kafka.avro.AvroModel;

import java.util.Objects;

public class AvroKey implements AvroModel {
    private String key;

    public AvroKey() {
    }

    public AvroKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvroKey avroKey = (AvroKey) o;
        return Objects.equals(key, avroKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
