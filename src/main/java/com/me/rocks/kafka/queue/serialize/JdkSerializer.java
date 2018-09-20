package com.me.rocks.kafka.queue.serialize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class JdkSerializer implements Serializer {
    private static final Logger log = LoggerFactory.getLogger(JdkSerializer.class);

    @Override
    public byte[] serialize(Object object) {
        if(object == null) {
            return null;
        }

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(baos);) {
            oos.writeObject(object);
            return baos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public <T> T deserialize(byte[] bytes) {
        if (bytes == null)
            return null;

        try(final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            final ObjectInputStream ois = new ObjectInputStream(bais);) {
            return (T) ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
