/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metrics2;

import com.codahale.metrics.Gauge;

public class RollingAverageGauge implements Gauge<Double> {
    private double[] samples = new double[3];
    private int index = 0;

    @Override
    public Double getValue() {
        synchronized (this) {
            double total = samples[0] + samples[1] + samples[2];
            return total / 3.0;
        }
    }

    public void addValue(double value) {
        synchronized (this) {
            samples[index] = value;
            index = (++index % 3);
        }
    }
}
