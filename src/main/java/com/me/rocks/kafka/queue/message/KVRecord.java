package com.me.rocks.kafka.queue.message;

import com.me.rocks.kafka.avro.AvroModel;

import java.io.Serializable;
import java.util.Objects;

public class KVRecord implements Serializable {
    private AvroModel key;
    private AvroModel value;

    public KVRecord() {
    }

    public KVRecord(AvroModel key, AvroModel value) {
        this.key = key;
        this.value = value;
    }

    public AvroModel getKey() {
        return key;
    }

    public void setKey(AvroModel key) {
        this.key = key;
    }

    public AvroModel getValue() {
        return value;
    }

    public void setValue(AvroModel value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "KVRecord{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KVRecord kvRecord = (KVRecord) o;
        return Objects.equals(key, kvRecord.key) &&
                Objects.equals(value, kvRecord.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
