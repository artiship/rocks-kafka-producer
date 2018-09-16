package com.me.rocks.kafka.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class KryoSerializer {
    private static final Logger log = LoggerFactory.getLogger(KryoSerializer.class);

    public byte[] serialize(Object obj) {
        if(obj == null) {
            return null;
        }

        byte[] bytes = null;
        try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//            final DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream);
            final Output output = new Output(byteArrayOutputStream);) {
            kryoLocal.get().writeClassAndObject(output, obj);
            output.flush();
            bytes = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            log.error("Kyro serialize failed", e);
        }

        return bytes;
    }


    public <T> T deserialize(byte[] bytes) {
        if(bytes == null) {
            return null;
        }

        try(final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            //final InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
            final Input input = new Input(byteArrayInputStream);) {
            return (T) kryoLocal.get().readClassAndObject(input);
        } catch (IOException e) {
            log.error("Kyro deserialize failed", e);
            return null;
        }
    }

    private static final ThreadLocal<Kryo> kryoLocal = ThreadLocal.withInitial(() -> new Kryo());

}