package com.flashsale.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/**
 * A clock that stands still until a test moves it, so "10 minutes later" takes no real time.
 */
public class MutableClock extends Clock {

    private volatile Instant now = freshNow();

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException("MutableClock is always UTC");
    }

    @Override
    public Instant instant() {
        return now;
    }

    public void advance(Duration duration) {
        now = now.plus(duration);
    }

    public void reset() {
        now = freshNow();
    }

    private static Instant freshNow() {
        return Instant.now().truncatedTo(ChronoUnit.MICROS);
    }
}
