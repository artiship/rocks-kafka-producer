package com.me.rocks.kafka;

import org.junit.Test;

public class RocksProducerShould extends AbstractShould{
    @Test public void
    should_send() {
        String topicName = "topic_name";
        RocksProducer producer = new RocksProducer(topicName);
        producer.send(user.getName(), user);
    }

}