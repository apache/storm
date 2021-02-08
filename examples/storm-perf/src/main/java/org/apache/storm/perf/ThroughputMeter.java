/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.perf;

public class ThroughputMeter {

    private String name;
    private long startTime = 0;
    private int count;
    private long endTime = 0;

    public ThroughputMeter(String name) {
        this.name = name;
        this.startTime = System.currentTimeMillis();
    }

    /**
     * Calculate throughput.
     * @return events/sec
     */
    private static double calcThroughput(long count, long startTime, long endTime) {
        long gap = (endTime - startTime);
        return (count / gap) * 1000;
    }

    public String getName() {
        return name;
    }

    public void record() {
        ++count;
    }

    public double stop() {
        if (startTime == 0) {
            return 0;
        }
        if (endTime == 0) {
            this.endTime = System.currentTimeMillis();
        }
        return calcThroughput(count, startTime, endTime);
    }

    // Returns the recorded throughput since the last call to getCurrentThroughput()
    //     or since this meter was instantiated if being called for fisrt time.
    public double getCurrentThroughput() {
        if (startTime == 0) {
            return 0;
        }
        long currTime = (endTime == 0) ? System.currentTimeMillis() : endTime;

        double result = calcThroughput(count, startTime, currTime) / 1000; // K/sec
        startTime = currTime;
        count = 0;
        return result;
    }
}
