package com.me.rocks.kafka.jmx;

import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.queue.jmx.RocksMetrics;

import java.util.concurrent.atomic.AtomicLong;

public class RocksProducerMetric extends RocksMetrics implements RocksProducerMetricMXBean, RocksProducer.Listener {
    private final AtomicLong sendSuccessCount = new AtomicLong();
    private final AtomicLong sendFailCount = new AtomicLong();
    private final String topic;

    public RocksProducerMetric(String topic) {
        this.topic = topic;
    }

    @Override
    protected String getObjectName() {
        return new StringBuilder("rocks.kafka.producer:topic=")
                .append(topic)
                .toString();
    }

    @Override
    public long getSendKafkaSucessCount() {
        return sendSuccessCount.get();
    }

    @Override
    public long getSendKafkaFailCount() {
        return sendFailCount.get();
    }

    @Override
    public void reset() {
        sendSuccessCount.set(0);
        sendFailCount.set(0);
    }

    @Override
    public void beforeSend(String topic, String message) {

    }

    @Override
    public void afterSend(String topic, String message) {

    }

    @Override
    public void onSendSuccess(String topic, long offset) {
        this.sendSuccessCount.incrementAndGet();
    }

    @Override
    public void onSendFail(String topic, String message, Exception exception) {
        this.sendFailCount.incrementAndGet();
    }
}
