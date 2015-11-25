/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.common.metric.old;

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Use com.codahale.metrics's interface
 * 
 * @author zhongyan.feng
 * 
 */
public class Timer extends Histogram {
    private static final long serialVersionUID = 5915881891513771108L;

    /**
     * A timing context.
     * 
     * @see Timer#time()
     */
    public static class Context implements Closeable {
        private final Timer timer;
        private final long startTime;

        private Context(Timer timer) {
            this.timer = timer;
            this.startTime = System.currentTimeMillis();
        }

        /**
         * Stops recording the elapsed time, updates the timer and returns the elapsed time in nanoseconds.
         */
        public long stop() {
            final long elapsed = System.currentTimeMillis() - startTime;
            timer.update(elapsed, TimeUnit.MILLISECONDS);
            return elapsed;
        }

        @Override
        public void close() {
            stop();
        }
    }

    public Timer() {
        init();
    }

    /**
     * Adds a recorded duration.
     * 
     * @param duration the length of the duration
     * @param unit the scale unit of {@code duration}
     */
    public void update(long duration, TimeUnit unit) {
        update(unit.toMillis(duration));
    }

    /**
     * Times and records the duration of event.
     * 
     * @param event a {@link Callable} whose {@link Callable#call()} method implements a process whose duration should be timed
     * @param <T> the type of the value returned by {@code event}
     * @return the value returned by {@code event}
     * @throws Exception if {@code event} throws an {@link Exception}
     */
    public <T> T time(Callable<T> event) throws Exception {
        final long startTime = System.currentTimeMillis();
        try {
            return event.call();
        } finally {
            update(System.currentTimeMillis() - startTime);
        }
    }

    /**
     * Returns a new {@link Context}.
     * 
     * @return a new {@link Context}
     * @see Context
     */
    public Context time() {
        return new Context(this);
    }

    public long getCount() {
        return allWindow.getSnapshot().getTimes();
    }
}
