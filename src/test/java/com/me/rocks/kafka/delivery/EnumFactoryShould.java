package com.me.rocks.kafka.delivery;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class EnumFactoryShould {
    private static final Logger log = (Logger) LoggerFactory.getLogger(EnumFactoryShould.class);
    private final ListAppender<ILoggingEvent> listAppender = new ListAppender<>();

    @Before public void
    up() {
        listAppender.start();
        log.addAppender(listAppender);
    }

    @Test public void
    enum_will_not_initialized_when_do_nothing() {
        List<ILoggingEvent> logsList = listAppender.list;

        assertEquals(logsList.size(), 0);
        /*List<ILoggingEvent> logsList = listAppender.list;

        assertEquals("Init enum GOOD", logsList.get(0).getFormattedMessage());
        assertEquals("Init class Ugly", logsList.get(1).getFormattedMessage());
        assertEquals("Init enum BAD", logsList.get(2).getFormattedMessage());
        assertEquals("Init class Ugly", logsList.get(3).getFormattedMessage());*/
    }

    @Test public void
    enum_should_all_be_initialized_when_one_of_it_invoked() {
        Type bad = Type.BAD;

        List<ILoggingEvent> logsList = listAppender.list;

        assertEquals(logsList.size(), 4);

        assertEquals("Init enum GOOD", logsList.get(0).getFormattedMessage());
        assertEquals("Init class Ugly", logsList.get(1).getFormattedMessage());
        assertEquals("Init enum BAD", logsList.get(2).getFormattedMessage());
        assertEquals("Init class Ugly", logsList.get(3).getFormattedMessage());
    }

    @Test public void
    should_get_the_same_enum_twice_return_the_singleton() {
        Type first = Type.GOOD;
        Type twice = Type.GOOD;

        assertEquals(first, twice);
    }

    enum Type {
        GOOD {
            private final Ugly ugly = new Ugly();

            @Override
            Type wood() {
                return null;
            }
        },
        BAD {
            private final Ugly ugly = new Ugly();

            @Override
            Type wood() {
                return null;
            }
        };

        abstract Type wood();

        Type() {
            log.info("Init enum {}", this.name());
        }
    }

    static class Ugly {
        public Ugly() {
            log.info("Init class Ugly");
        }
    }

}