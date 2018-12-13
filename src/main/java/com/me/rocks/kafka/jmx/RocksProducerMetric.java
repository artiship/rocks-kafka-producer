package com.me.rocks.kafka.jmx;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.me.rocks.kafka.RocksProducer;
import com.me.rocks.queue.jmx.RocksMetrics;

import java.util.concurrent.atomic.AtomicLong;

public class RocksProducerMetric extends RocksMetrics implements RocksProducerMetricMXBean, RocksProducer.Listener {
    private final MetricRegistry metricsRegistry = new MetricRegistry();
    private final AtomicLong deliverySuccessCount = new AtomicLong();
    private final AtomicLong deliveryFailCount = new AtomicLong();
    private final AtomicLong deliveryFailDiscardCount = new AtomicLong();
    private final RocksProducer rocksProducer;
    private final AtomicLong deliveryTime = new AtomicLong(0);
    private final Meter deliveryMeter;
    private final Histogram deliveryHis;
    private final Meter sendMeter;

    public RocksProducerMetric(RocksProducer rocksProducer) {
        this.rocksProducer = rocksProducer;
        this.deliveryMeter = this.metricsRegistry.meter("deliveryMeter");
        this.deliveryHis = this.metricsRegistry.histogram("deliveryHis");
        this.sendMeter = this.metricsRegistry.meter("sendMeter");
    }

    @Override
    protected String getObjectName() {
        return new StringBuilder("rocks.kafka.producer:topic=")
                .append(rocksProducer.getTopic())
                .toString();
    }

    @Override
    public long getKafkaDeliverySuccessCount() {
        return deliverySuccessCount.get();
    }

    @Override
    public long getKafkaDeliveryFailCount() {
        return deliveryFailCount.get();
    }

    @Override
    public long getKafkaDeliveryFailDiscardCount() {
        return this.deliveryFailDiscardCount.get();
    }

    @Override
    public boolean isKafkaBrokersAvailable() {
        return rocksProducer.isKafkaBrokersAvailable();
    }

    @Override
    public boolean isKafkaTopicAvailable() {
        return rocksProducer.isKafkaTopicAvailable();
    }

    @Override
    public boolean isSchemaRegistryAvailable() {
        return rocksProducer.isSchemaRegistryAvailable();
    }

    @Override
    public String getTopic() {
        return this.rocksProducer.getTopic();
    }

    @Override
    public String getKafkaDeliveryMode() {
        return rocksProducer.getDeliveryMode();
    }

    @Override
    public void reset() {
        deliverySuccessCount.set(0);
        deliveryFailCount.set(0);
        deliveryFailDiscardCount.set(0);
    }

    @Override
    public void onSend() {
        this.sendMeter.mark();
    }

    @Override
    public void beforeDelivery(String topic, String message) {
        this.deliveryTime.set(System.currentTimeMillis());
    }

    @Override
    public void afterDelivery(String topic, String message) {
        if(this.deliveryTime.get() > 0) {
            this.deliveryHis.update(System.currentTimeMillis() - this.deliveryTime.get());
        }
    }

    @Override
    public void onDeliverySuccess(String topic, long offset) {
        this.deliverySuccessCount.incrementAndGet();
        this.deliveryMeter.mark();
    }

    @Override
    public void onDeliveryFail(String topic, String message, Exception exception) {
        this.deliveryFailCount.incrementAndGet();
    }

    @Override
    public void onDeliveryFailDiscard(String topic, String message) {
        this.deliveryFailDiscardCount.incrementAndGet();
    }

    @Override
    public double getKafkaDeliverySuccessFifteenMinuteRate() {
        return this.deliveryMeter.getFifteenMinuteRate();
    }

    @Override
    public double getKafkaDeliverySuccessFiveMinuteRate() {
        return this.deliveryMeter.getFiveMinuteRate();
    }

    @Override
    public double getKafkaDeliverySuccessOneMinuteRate() {
        return this.deliveryMeter.getOneMinuteRate();
    }

    @Override
    public double getKafkaDeliverySuccessMeanRate() {
        return this.deliveryMeter.getMeanRate();
    }

    @Override
    public double getKafkaDeliveryTime95thPercentile() {
        return this.deliveryHis.getSnapshot().get95thPercentile();
    }

    @Override
    public double getRocksProducerSendFifteenMinuteRate() {
        return this.sendMeter.getFifteenMinuteRate();
    }

    @Override
    public double getRocksProducerSendFiveMinuteRate() {
        return this.sendMeter.getFiveMinuteRate();
    }

    @Override
    public double getRocksProducerSendOneMinuteRate() {
        return this.sendMeter.getOneMinuteRate();
    }

    @Override
    public double getRocksProducerSendMeanRate() {
        return this.sendMeter.getMeanRate();
    }

    @Override
    public long getRocksProducerSendCount() {
        return this.sendMeter.getCount();
    }
}
