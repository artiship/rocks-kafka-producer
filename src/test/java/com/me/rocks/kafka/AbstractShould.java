package com.me.rocks.kafka;

import com.me.rocks.kafka.benchmark.model.DifferentUser;
import com.me.rocks.kafka.message.KVRecord;
import org.junit.Before;

abstract public class AbstractShould {

    protected DifferentUser user;
    protected KVRecord kv;

    @Before public void
    init() {
        user = DifferentUser.mock();
        kv = new KVRecord(user.getName(), user);
    }
}