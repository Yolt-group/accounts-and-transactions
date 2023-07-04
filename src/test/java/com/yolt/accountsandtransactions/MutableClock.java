package com.yolt.accountsandtransactions;

import java.time.*;

public class MutableClock extends Clock {

    private Clock base = Clock.systemUTC();

    public void asFixed(final LocalDateTime pointInTime) {
        base = Clock.fixed(pointInTime.toInstant(ZoneOffset.UTC), ZoneId.of("UTC"));
    }

    public void reset() {
        this.base = Clock.systemUTC();
    }

    @Override
    public ZoneId getZone() {
        return base.getZone();
    }

    @Override
    public Clock withZone(ZoneId zoneId) {
        return base.withZone(zoneId);
    }

    @Override
    public Instant instant() {
        return Instant.now(base);
    }
}
