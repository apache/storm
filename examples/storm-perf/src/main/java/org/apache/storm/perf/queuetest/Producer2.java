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

package org.apache.storm.perf.queuetest;

import org.apache.storm.utils.JCQueue;

/**
 * Writes to two queues.
 */
class Producer2 extends MyThread {
    private final JCQueue q1;
    private final JCQueue q2;

    Producer2(JCQueue q1, JCQueue q2) {
        super("Producer2");
        this.q1 = q1;
        this.q2 = q2;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            while (!Thread.interrupted()) {
                q1.publish(++count);
                q2.publish(count);
            }
            runTime = System.currentTimeMillis() - start;
        } catch (InterruptedException e) {
            return;
        }

    }
}