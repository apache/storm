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
package com.alibaba.jstorm.common.metric;

import com.codahale.metrics.Gauge;

/**
 * 
 * @author zhongyan.feng
 * @version $Id:
 */
public class TimerRatio implements Gauge<Double> {

    private long lastUpdateTime = 0;
    private long sum = 0;
    private long lastGaugeTime;

    public void init() {
        lastGaugeTime = System.nanoTime();
    }

    public synchronized void start() {
        if (lastUpdateTime == 0) {
            lastUpdateTime = System.nanoTime();
        }
    }

    public synchronized void stop() {
        if (lastUpdateTime != 0) {
            long now = System.nanoTime();
            long cost = now - lastUpdateTime;
            lastUpdateTime = 0;
            sum += cost;
        }

    }

    @Override
    public Double getValue() {
        synchronized (this) {
            stop();

            long now = System.nanoTime();
            long cost = now - lastGaugeTime;
            if (cost == 0) {
                return 1.0;
            }

            lastGaugeTime = now;
            double ratio = ((double) sum) / cost;
            sum = 0;
            return ratio;

        }

    }

}
