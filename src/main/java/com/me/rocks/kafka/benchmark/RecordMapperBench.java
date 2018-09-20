package com.me.rocks.kafka.benchmark;

import com.me.rocks.kafka.avro.GenericRecordMapper;
import com.me.rocks.kafka.benchmark.model.DifferentUser;
import org.apache.avro.generic.GenericData;
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
@State(Scope.Benchmark)
public class RecordMapperBench {
    private static final Logger log = LoggerFactory.getLogger(RecordMapperBench.class);
    private DifferentUser user;
    private GenericData.Record record;
    private DifferentUser differentUser;

    @Setup(Level.Trial)
    public void setUp() {
        log.info("Initialize serializer benchmark variables");

        user = DifferentUser.mock();
        record = GenericRecordMapper.mapObjectToRecord(user);
        differentUser = new DifferentUser();
    }

    @TearDown
    public void tearDown() {
        log.info("Destroy serializer benchmark variables");
    }


    @Benchmark
    public void mapObjectToRecord() {
        GenericRecordMapper.mapObjectToRecord(user);
    }

    @Benchmark
    public void mapRecordToObject() {
        GenericRecordMapper.mapRecordToObject(record, differentUser);
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RecordMapperBench.class.getSimpleName()+ ".*")
                .timeUnit(MICROSECONDS)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}


