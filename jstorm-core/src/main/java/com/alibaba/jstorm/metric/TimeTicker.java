package com.alibaba.jstorm.metric;

import java.util.concurrent.TimeUnit;

/**
 * a simple util class to calculate run time
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class TimeTicker {
    private TimeUnit unit;
    private long start;
    private long end;

    public TimeTicker(TimeUnit unit) {
        if (unit != TimeUnit.NANOSECONDS && unit != TimeUnit.MILLISECONDS) {
            throw new IllegalArgumentException("invalid unit!");
        }
        this.unit = unit;
    }

    public TimeTicker(TimeUnit unit, boolean start) {
        this(unit);
        if (start) {
            start();
        }
    }

    public void start() {
        if (unit == TimeUnit.MILLISECONDS) {
            this.start = System.currentTimeMillis();
        } else if (unit == TimeUnit.NANOSECONDS) {
            this.start = System.nanoTime();
        }
    }

    public long stop() {
        if (unit == TimeUnit.MILLISECONDS) {
            this.end = System.currentTimeMillis();
        } else if (unit == TimeUnit.NANOSECONDS) {
            this.end = System.nanoTime();
        }
        return end - start;
    }

    public long stopAndRestart() {
        long elapsed = stop();
        start();
        return elapsed;
    }
}
