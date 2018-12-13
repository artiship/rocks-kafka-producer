package com.me.rocks.kafka;

import com.me.rocks.kafka.benchmark.model.DifferentUser;
import com.me.rocks.kafka.delivery.health.KafkaHealthChecker;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategy;
import com.me.rocks.kafka.delivery.strategies.DeliveryStrategyFast;
import com.me.rocks.kafka.queue.message.AvroKey;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.queue.RocksStore;
import com.me.rocks.queue.StoreOptions;
import com.me.rocks.queue.util.Files;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MockKafkaAdminClientEnv;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

abstract public class AbstractShould {

    protected DifferentUser user;
    protected KVRecord kv;
    protected static final String topic = "topic_name";
    protected static final String rocks_db = "rocks_db";
    protected MockProducer<Record, Record> mockProducer;
    protected DeliveryStrategy deliveryStrategy;
    protected RocksProducer rocksProducer;
    protected RocksStore store;
    protected KafkaHealthChecker kafkaHealthChecker;

    @Before public void
    setUp() {
        user = DifferentUser.mock();
        kv = new KVRecord(new AvroKey(user.getName()), user);

        Serializer mockAvroSerializer = mockAvroSerializer();
        mockProducer = new MockProducer<Record, Record>(true, mockAvroSerializer, mockAvroSerializer);
        deliveryStrategy = new DeliveryStrategyFast(mockProducer);
        store = new RocksStore(StoreOptions.builder().database(rocks_db).build());
        kafkaHealthChecker = new MockKafkaHealthChecker();

        rocksProducer = RocksProducer.builder()
                .topic(topic)
                .kafkaDeliveryStrategy(deliveryStrategy)
                .rocksStore(store)
                .kafkaHealthChecker(kafkaHealthChecker)
                .build();
    }

    protected void waitForAwhile() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public AdminClient mockAdminClient() {
        HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));
        nodes.put(1, new Node(1, "localhost", 8122));
        nodes.put(2, new Node(2, "localhost", 8123));

        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        MockKafkaAdminClientEnv mockKafkaAdminClientEnv = new MockKafkaAdminClientEnv(cluster);
        return mockKafkaAdminClientEnv.adminClient();
    }

    public Serializer mockAvroSerializer() {
        Properties pro = new Properties();
        pro.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        return new KafkaAvroSerializer(schemaRegistry, new HashMap(pro));
    }

    public class MockKafkaHealthChecker extends KafkaHealthChecker {
        public MockKafkaHealthChecker() {
        }

        public boolean isKafkaTopicAvailable(final String topic) {
            return true;
        }

        public boolean isKafkaBrokersAvailable() {
            return true;
        }

        public boolean isSchemaRegistryAvailable() {
            return true;
        }
    }

    @After
    public void
    destroy() {
        this.store.close(); //release rocks db lock for directory deleting
        this.rocksProducer.getRocksProducerMetric().unregister();
        Files.deleteDirectory(rocks_db);
    }
}