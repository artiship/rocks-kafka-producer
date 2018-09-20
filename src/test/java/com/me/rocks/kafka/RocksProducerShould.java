package com.me.rocks.kafka;

import com.me.rocks.kafka.delivery.DeliveryStrategyEnum;
import com.me.rocks.kafka.exception.RocksProducerException;
import com.me.rocks.kafka.queue.serialize.JdkSerializer;
import org.junit.Test;

public class RocksProducerShould extends AbstractShould{
    @Test public void
    should_send() {
        String topic = "topic_name";

        RocksProducer producer = RocksProducer.builder()
                .topic(topic)
                .serializer(new JdkSerializer())
                .kafkaDeliveryStrategy(DeliveryStrategyEnum.FAST)
                .build();
        try {
            producer.send(user.getName(), user);
        } catch (RocksProducerException e) {
            e.printStackTrace();
        }
    }

}