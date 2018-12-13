package com.me.rocks.kafka.jmx;

import com.me.rocks.kafka.AbstractShould;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;

public class RocksProducerMetricShould extends AbstractShould {
    private static final Logger log = LoggerFactory.getLogger(RocksProducerMetricShould.class);

    private MBeanServer mBeanServer;
    private ObjectName objectName;

    @Test
    public void
    when_create_queue_should_should_register_jmx() throws Exception {
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        objectName = new ObjectName("rocks.kafka.producer:topic=" + topic);

        assertEquals(mBeanServer.getAttribute(objectName, "KafkaDeliverySuccessCount"), 0L);
        assertEquals(mBeanServer.getAttribute(objectName, "KafkaDeliveryFailCount"), 0L);
    }

}