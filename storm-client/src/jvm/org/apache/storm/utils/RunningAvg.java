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

package org.apache.storm.utils;

public class RunningAvg {

    private long n = 0;
    private double oldM, newM, oldS, newS;
    private String name;
    public static int printFreq = 20_000_000;
    private boolean disable;
    private long count = 0;

    public RunningAvg(String name, boolean disable) {
        this(name, printFreq, disable);
    }

    public RunningAvg(String name, int printFreq) {
        this(name, printFreq, false);
    }

    public RunningAvg(String name, int printFreq, boolean disable) {
        this.name = name + "_" + Thread.currentThread().getName();
        this.printFreq = printFreq;
        this.disable = disable;
    }

    public void clear() {
        n = 0;
    }

    public void pushLatency(long startMs) {
        push(System.currentTimeMillis() - startMs);
    }

    public void push(long x) {
        if (disable) {
            return;
        }

        n++;

        if (n == 1) {
            oldM = newM = x;
            oldS = 0;
        } else {
            newM = oldM + (x - oldM) / n;
            newS = oldS + (x - oldM) * (x - newM);

            // set up for next iteration
            oldM = newM;
            oldS = newS;
        }
        if (++count == printFreq) {
            System.err.printf("  ***> %s - %,.2f\n", name, mean());
            count = 0;
        }
    }

    public long numDataValues() {
        return n;
    }

    public double mean() {
        return (n > 0) ? newM : 0.0;
    }

    public double variance() {
        return ((n > 1) ? newS / (n - 1) : 0.0);
    }

}