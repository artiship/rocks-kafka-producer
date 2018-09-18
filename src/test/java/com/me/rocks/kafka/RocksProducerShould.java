package com.me.rocks.kafka;

import org.junit.Test;

import java.net.DatagramSocket;

import static org.junit.Assert.*;

public class RocksProducerShould extends AbstractShould{
    @Test public void
    should_send() {
        String topicName = "topic_name";
        RocksProducer producer = new RocksProducer(topicName);
        producer.send(user.getName(), user);
    }

}