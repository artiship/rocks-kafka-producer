package com.me.rocks.kafka;

import com.me.rocks.kafka.delivery.DeliveryStrategies;
import com.me.rocks.kafka.exception.RocksProducerException;
import com.me.rocks.kafka.queue.serialize.JdkSerializer;
import org.junit.Test;

public class RocksProducerShould extends AbstractShould{
    @Test public void
    should_send() {
        String topicName = "topic_name";

        RocksProducer producer = RocksProducer.builder()
                .topic(topicName)
                .serializer(new JdkSerializer())
                .kafkaDeliveryStrategy(DeliveryStrategies.FAST)
                .build();
        try {
            producer.send(user.getName(), user);
        } catch (RocksProducerException e) {
            e.printStackTrace();
        }
    }

}