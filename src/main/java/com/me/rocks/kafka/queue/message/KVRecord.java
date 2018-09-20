package com.me.rocks.kafka.queue.message;

import com.me.rocks.kafka.avro.AvroModel;

import java.io.Serializable;
import java.util.Objects;

public class KVRecord implements Serializable {
    private String key;
    private AvroModel model;

    public KVRecord() {
    }

    public KVRecord(String key, AvroModel model) {
        this.key = key;
        this.model = model;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public AvroModel getModel() {
        return model;
    }

    public void setModel(AvroModel model) {
        this.model = model;
    }

    @Override
    public String toString() {
        return "KVRecord{" +
                "key='" + key + '\'' +
                ", model=" + model +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KVRecord kvRecord = (KVRecord) o;
        return Objects.equals(key, kvRecord.key) &&
                Objects.equals(model, kvRecord.model);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, model);
    }
}
