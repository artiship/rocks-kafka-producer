package com.me.rocks.kafka.jmx;

import com.me.rocks.queue.jmx.RocksMetrics;

import java.util.concurrent.atomic.AtomicLong;

public class RocksProducerMetric extends RocksMetrics implements RocksProducerMetricMXBean {
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

    public void onSendSucess() {
        this.sendSuccuessCount.incrementAndGet();
    }

    public void onSendFail() {
        this.sendFailCount.incrementAndGet();
    }
}
