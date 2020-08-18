/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metrics2;

import org.junit.Assert;
import org.junit.Test;

public class RollingAverageGaugeTest {

    @Test
    public void testAverage() {
        RollingAverageGauge gauge = new RollingAverageGauge();
        Assert.assertEquals(0.0, gauge.getValue(), 0.001);
        gauge.addValue(30);
        Assert.assertEquals(10.0, gauge.getValue(), 0.001);
        gauge.addValue(30);
        Assert.assertEquals(20.0, gauge.getValue(), 0.001);
        gauge.addValue(30);
        Assert.assertEquals(30.0, gauge.getValue(), 0.001);
        gauge.addValue(90);
        Assert.assertEquals(50.0, gauge.getValue(), 0.001);
        gauge.addValue(0);
        Assert.assertEquals(40.0, gauge.getValue(), 0.001);
    }
}