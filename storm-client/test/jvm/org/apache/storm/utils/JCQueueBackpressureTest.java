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
package org.apache.storm.utils;

import org.apache.storm.policy.WaitStrategyPark;
import org.apache.storm.utils.JCQueue.Consumer;
import org.apache.storm.utils.JCQueue.ProducerKind;
import org.junit.Test;
import junit.framework.TestCase;


// TODO: ROSHAN: Revise this test
public class JCQueueBackpressureTest extends TestCase {

    private final static int MESSAGES = 100;
    private final static int CAPACITY = 128;

    @Test
    public void testBackPressureCallback() throws Exception {

        final JCQueue queue = createQueue("testBackPressure", CAPACITY);

        for (int i = 0; i < MESSAGES; i++) {
            queue.publish(String.valueOf(i));
        }


        queue.consume(new Consumer() {
            @Override
            public void accept(Object o) {
//                 consumerCursor.set(l);
            }

            @Override
            public void flush() throws InterruptedException
            { }
        });

    }

    private static JCQueue createQueue(String name, int queueSize) {
        return new JCQueue(name, ProducerKind.MULTI, queueSize, 1, new WaitStrategyPark(100));
    }
}
