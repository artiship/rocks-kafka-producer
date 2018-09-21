package com.me.rocks.kafka;

import com.me.rocks.kafka.exception.RocksProducerException;
import org.junit.Test;

public class RocksProducerShould extends AbstractShould{
    @Test public void
    should_send() {
        String topic = "topic_name";
        RocksProducer producer = RocksProducer.create(topic);
        try {
            producer.send(user.getName(), user);
        } catch (RocksProducerException e) {
            e.printStackTrace();
        }
    }

}