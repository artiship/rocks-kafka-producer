package com.me.rocks.kafka.delivery.strategies;

import com.me.rocks.kafka.config.KafkaProducerConfig;
import com.me.rocks.queue.RocksQueue;

import java.util.Properties;

public class DeliveryStrategyFast extends DeliveryStrategyTemplate {
    @Override
    public void onCallBack(final RocksQueue queue) {

    }

    @Override
    public void afterSend(final RocksQueue queue) {
        this.removeHead(queue);
    }

    @Override
    public Properties getProducerProperties() {
        return KafkaProducerConfig.loadProperties();
    }
}
