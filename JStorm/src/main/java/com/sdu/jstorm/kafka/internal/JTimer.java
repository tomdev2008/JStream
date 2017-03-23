package com.sdu.jstorm.kafka.internal;

import lombok.Getter;

import java.util.concurrent.TimeUnit;

/**
 * @author hanhan.zhang
 * */
@Getter
public class JTimer {

    private long delay;
    private long period;
    private TimeUnit timeUnit;
    private long periodNanos;
    private long start;

    public JTimer(long delay, long period, TimeUnit timeUnit) {
        this.delay = delay;
        this.period = period;
        this.timeUnit = timeUnit;

        periodNanos = timeUnit.toNanos(period);
        start = System.nanoTime() +  timeUnit.toNanos(delay);
    }

    public boolean isExpiredResetOnTrue() {
        boolean expired = System.nanoTime() - start >= periodNanos;

        if (expired) {
            start = System.nanoTime();
        }

        return expired;
    }

}
