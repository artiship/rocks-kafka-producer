package com.me.rocks.kafka.jmx;

import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.queue.jmx.RocksMetrics;

import java.util.concurrent.atomic.AtomicLong;

public class RocksProducerMetric extends RocksMetrics implements RocksProducerMetricMXBean, RocksProducer.Listener {
    private AtomicLong sendSuccuessCount = new AtomicLong();
    private AtomicLong sendFailCount = new AtomicLong();

    @Override
    protected String getObjectName() {
        return null;
    }

    @Override
    public String getQueueName() {
        return null;
    }

    @Override
    public long getSendKafkaSucessCount() {
        return 0;
    }

    @Override
    public long getSendKafkaFailCount() {
        return 0;
    }

    @Override
    public void reset() {

    }


    @Override
    public void beforeSend(long index) {

    }

    @Override
    public void onSendSuccess(String topic, long offset) {

    }

    @Override
    public void onSendFail(String topic, String message, Exception exception) {

    }

    @Override
    public void afterSend(long index) {

    }
}
