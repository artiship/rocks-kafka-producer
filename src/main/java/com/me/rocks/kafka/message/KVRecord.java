package com.me.rocks.kafka.message;

import org.apache.avro.generic.GenericData;

import java.io.Serializable;

public class KVRecord implements Serializable {
    private String key;
    private GenericData.Record record;

    public KVRecord() {
    }

    public KVRecord(String key, GenericData.Record record) {
        this.key = key;
        this.record = record;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public GenericData.Record getRecord() {
        return record;
    }

    public void setRecord(GenericData.Record record) {
        this.record = record;
    }
}
