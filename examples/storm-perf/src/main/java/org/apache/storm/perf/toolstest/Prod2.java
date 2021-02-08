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

package org.apache.storm.perf.toolstest;

import org.jctools.queues.MpscArrayQueue;

/**
 * Writes to two queues.
 */
class Prod2 extends MyThd {
    private final MpscArrayQueue<Object> q1;
    private final MpscArrayQueue<Object> q2;

    Prod2(MpscArrayQueue<Object> q1, MpscArrayQueue<Object> q2) {
        super("Producer2");
        this.q1 = q1;
        this.q2 = q2;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();

        while (!halt) {
            q1.offer(++count);
            q2.offer(count);
        }
        runTime = System.currentTimeMillis() - start;
    }
}
