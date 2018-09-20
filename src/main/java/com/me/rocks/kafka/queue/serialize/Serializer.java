package com.me.rocks.kafka.queue.serialize;

public interface Serializer {
    public byte[] serialize(Object obj);
    public <T> T deserialize(byte[] bytes);
}
