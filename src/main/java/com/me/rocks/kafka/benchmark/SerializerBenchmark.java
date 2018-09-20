package com.me.rocks.kafka.benchmark;

import com.me.rocks.kafka.benchmark.model.DifferentUser;
import com.me.rocks.kafka.queue.message.KVRecord;
import com.me.rocks.kafka.queue.serialize.JdkSerializer;
import com.me.rocks.kafka.queue.serialize.KryoSerializer;
import com.me.rocks.kafka.queue.serialize.Serializer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(2)
@State(Scope.Benchmark)
public class SerializerBenchmark {
    private static final Logger log = LoggerFactory.getLogger(RecordMapperBench.class);

    private Serializer jdkSerializer;
    private Serializer kryoSerializer;
    private KVRecord kvRecord;
    private byte[] jdkSerialized;
    private byte[] kryoSerialized;

    @Setup(Level.Trial)
    public void setUp() {
        log.info("Initialize serializer benchmark variables");

        jdkSerializer = new JdkSerializer();
        kryoSerializer = new KryoSerializer();

        kvRecord = new KVRecord();
        DifferentUser user = DifferentUser.mock();
        kvRecord.setKey(String.valueOf(user.getName()));
        kvRecord.setModel(user);

        jdkSerialized = jdkSerializer.serialize(kvRecord);
        kryoSerialized = kryoSerializer.serialize(kvRecord);
    }

    @TearDown
    public void tearDown() {
        log.info("Destroy serializer benchmark variables");
    }


    @Benchmark
    public void jdkSer() {
        jdkSerializer.serialize(kvRecord);
    }

    @Benchmark
    public void jdkDe() {
        jdkSerializer.deserialize(jdkSerialized);
    }

    @Benchmark
    public void kryoSer() {
        kryoSerializer.serialize(kvRecord);
    }

    @Benchmark
    public void kryoDe() {
        kryoSerializer.deserialize(kryoSerialized);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SerializerBenchmark.class.getSimpleName()+ ".*")
                .timeUnit(MICROSECONDS)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
