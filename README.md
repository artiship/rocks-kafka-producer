# rocks-kafka-producer

A rocks solid kafka producer with persistent queue

## Why persistent queue for kafka producer?

Kafka clients facing two scenarios should concern about:

1. When kafka cluster or brokers are not available, clients are continuing sending data to. This will results in client has too much data resides in memory which can not be delivered to kafka. This will exhaust client's memory and even cause it crash. Then if restart the application, data in memory will lose.
2. When client application suddenly crash, data reside in kafka client's memory accumulator will lose.

These two scenarios could be addressed by the persistent queue. This project uses the another's another project `rocks-queue-java` provides embedded local persistent queue for sending data. 

Data will first sync write into rocks queue as well as a write-ahead-log. Meanwhile there is a thread forever asynchronously consuming the queue and then delivery messages to kafka. 

1. If kafka brokers are not available, data sent by clients will stored in RocksDB at local file system. Once the kafka cluster has been recovered. Data at last will be consumed and written into kafka.
2. In the case client crash. Since data are first written into write-ahead-log. Data will be recovered after client restart.

Please notice this producer is dedicated for Confluent Kafka Platform with schema registry and support Avro Schema.

## Two kinds of sending mode

For different requirements, this project provides two kinds of sending mode: `fast` and `reliable`

1. `reliable` mode will not consume the next item from queue until the kafka send callback successfully return.
2. `fast` will not wait for callback return and continue to process the next message, this the trade off can made by client between efficiency and data lose

## Avro Schema

This project uses avro schema by default for data quality and handling schema evolution. It uses Avro's reflect API to automatically convert POJO to avro json, let the users don't need to concern about the sophisticated avro dsl syntax.

```json
@Data
public class SimpleModel implements AvroModel {
    private int id;
    private String name;
}
```

If use AvroUtils.getSchema(simpleModel) will generate schema bellow. This schema will automatically register into kafka schema registry when sending data to kafka.

```json
{
  "type": "record",
  "name": "SimpleModel",
  "namespace": "com.me.kafka.model",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    }
  ]
}
```

## Usage

#### Maven

Add maven dependency to pom.xml

```xml
<dependency>
    <groupId>com.me.rocks.kafka</groupId>
    <artifactId>rocks-kafka-producer</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
  
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>1.0.0</version>
</dependency>
```

#### Creating Rocks Producer 

One topic should only have one `RocksProducer`, multiple producers will let the queue consuming disordered. In case you uses factory, you can create producer as single instance like bellow:

```java
@Configuration
public class RocksProducersConfig {
    private final String TOPIC_RELIABLE = "topic_reliable";
    private final String TOPIC_FAST = "topic_fast";
 
    @Bean
    public RocksProducer reliableProducer() {
        return RocksProducer.createReliable(TOPIC_RELIABLE);
    }
 
    @Bean
    public RocksProducer fastProducer() {
        return RocksProducer.createReliable(TOPIC_FAST);
    }
}
```

By default the fast mode will be used when create a `RocksProducer`

```java
private final String TOPIC_FAST = "topic_fast";
private final RocksProducer fast = RocksProducer.create(TOPIC_FAST);
SimpleModel model = new SimpleModel();
try {
    fast.send(model);
} catch(RocksProducerException e) {
    log.err("send exception", e);
}
```

Or create different mode producers explicitly

```java
RocksProducer reliable = RocksProducer.createReliable(TOPIC_RELIABLE);
RocksProducer fast = RocksProducer.createFast(TOPIC_FAST);
```

Otherwise you can also use a builder to customize a more dedicated producer:

```java
RocksProducer rocksProducer = RocksProducer.builder()
                .topic(topic)
                .kafkaDeliveryStrategy(DeliveryStrategyEnum.FAST)
                .rocksStore(store)
                .kafkaHealthChecker(kafkaHealthChecker)
                .build();
```

## Life cycle listeners

RocksProducer provides life cycle listeners for monitoring:

```java
RocksProducer.Listener listener = new RocksProducer.Listener() {
    @Override
    public void beforeDelivery(String topic, String message) { log.info(...)}
    @Override
    public void afterDelivery(String topic, String message) {...}
    @Override
    public void onDeliverySuccess(String topic, long offset) {...}
    @Override
    public void onDeliveryFail(String topic, String message, Exception exception) {...}
    @Override
    public void onDeliveryFailDiscard(String topic, String message) {...}
};

rocksProducer.registerListener(listener);
```

## JMX

This project also provides a bunch of jmx metrics for monitoring:

Metric| Type| Desc
---|---:|---
Topic| String | topic 
KafkaDeliveryMode| String | delivery mode: fast, reliable
KafkaBrokersAvailable| boolean | kafka brokers are available or not 
KafkaTopicAvailable| boolean | kafka topic exists or not
SchemaRegistryAvailable| boolean | schema registry available or not
RocksProducerSendCount| long | producer send count
RocksProducerSendMeanRate| double | 
RocksProducerSendOneMinuteRate| double | 
RocksProducerSendFiveMinuteRate| double | 
RocksProducerSendFifteenMinuteRate| double | 
KafkaDeliverySuccessCount| long |
KafkaDeliveryFailCount| long | 
KafkaDeliveryFailDiscardCount| long | 
KafkaDeliverySuccessMeanRate| double | 
KafkaDeliverySuccessOneMinuteRate| double |  
KafkaDeliverySuccessFiveMinuteRate| double | 
KafkaDeliverySuccessFifteenMinuteRate| double | 
KafkaDeliveryTime95thPercentile| double | 


## Config parameters

`Kafka producer` configs 

Name	| value for example
-----|-----|
bootstrap.servers|	localhost:9092 |
acks	|all	
retries	|10	
buffer.memory	|33554432	
batch.size	|16384	
linger.ms	|5	
max.request.size|1048576
compression.type|gzip
key.serializer|io.confluent.kafka.serializers.KafkaAvroSerializer
value.serializer|io.confluent.kafka.serializers.KafkaAvroSerializer
schema.registry.url|http://localhost:8081

`RocksDB` configs

name	| value for example
-----|-----
rocksdb.location.directory|/data/rocks_db/
rocksdb.database.name|rocks_db
rocksdb.memeory.size|8388608
rocksdb.file.size.base
rocksdb.write.buffer.size
rocksdb.write.buffer.number
rocksdb.parallel.number

## Benchmark testing

Benchmark|                            Mode|  Cnt|    Score|     Error|  Units
---|---|---|---:|---:|---
RecordMapperBench.mapObjectToRecord|  avgt|   20|  194.074| ±  59.699|  us/op
RecordMapperBench.mapRecordToObject|  avgt|   20|  892.696| ± 431.296|  us/op
SerializerBenchmark.jdkDe|            avgt|   20|  712.803| ± 665.304|  us/op
SerializerBenchmark.jdkSer|           avgt|   20|  279.091| ± 760.410|  us/op
SerializerBenchmark.kryoDe|           avgt|   20|  105.955| ± 104.083|  us/op
SerializerBenchmark.kryoSer|          avgt|   20|   58.176| ±  43.003|  us/op

