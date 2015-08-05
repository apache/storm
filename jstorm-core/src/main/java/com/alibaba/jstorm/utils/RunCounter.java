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
package com.alibaba.jstorm.utils;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunCounter implements Serializable {

    private static final long serialVersionUID = 2177944366059817622L;
    private static final Logger LOG = LoggerFactory.getLogger(RunCounter.class);
    private AtomicLong total = new AtomicLong(0);
    private AtomicLong times = new AtomicLong(0);
    private AtomicLong values = new AtomicLong(0);

    private IntervalCheck intervalCheck;

    private final String id;

    public RunCounter() {
        this("", RunCounter.class);
    }

    public RunCounter(String id) {
        this(id, RunCounter.class);
    }

    public RunCounter(Class tclass) {
        this(tclass.getName(), tclass);

    }

    public RunCounter(String id, Class tclass) {
        this.id = id;

        intervalCheck = new IntervalCheck();
        intervalCheck.setInterval(60);
    }

    public Double count(long value) {
        long totalValue = total.incrementAndGet();
        long timesValue = times.incrementAndGet();
        long v = values.addAndGet(value);

        Double pass = intervalCheck.checkAndGet();
        if (pass != null) {
            times.set(0);
            values.set(0);

            Double tps = timesValue / pass;

            StringBuilder sb = new StringBuilder();
            sb.append(id);
            sb.append(", tps:" + tps);
            sb.append(", avg:" + ((double) v) / timesValue);
            sb.append(", total:" + totalValue);
            LOG.info(sb.toString());

            return tps;
        }

        return null;
    }

    public void cleanup() {

        LOG.info(id + ", total:" + total);
    }

    public IntervalCheck getIntervalCheck() {
        return intervalCheck;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
