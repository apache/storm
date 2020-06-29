/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.utils;

import java.util.Collections;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.policy.WaitStrategyPark;
import org.apache.storm.utils.JCQueue.Consumer;
import org.junit.Assert;
import org.junit.Test;

public class JCQueueBackpressureTest {
    
    private static JCQueue createQueue(String name, int queueSize) {
        return new JCQueue(name, name, queueSize, 0, 1, new WaitStrategyPark(0), "test", "test", Collections.singletonList(1000), 1000, new StormMetricRegistry());
    }

    @Test
    public void testNoReOrderingUnderBackPressure() throws Exception {
        final int MESSAGES = 100;
        final int CAPACITY = 64;

        final JCQueue queue = createQueue("testBackPressure", CAPACITY);

        for (int i = 0; i < MESSAGES; i++) {
            if (!queue.tryPublish(i)) {
                Assert.assertTrue(queue.tryPublishToOverflow(i));
            }
        }

        TestConsumer consumer = new TestConsumer();
        queue.consume(consumer);
        Assert.assertEquals(MESSAGES, consumer.lastMsg);
        
        queue.close();
    }

    // check that tryPublish() & tryOverflowPublish() work as expected
    @Test
    public void testBasicBackPressure() throws Exception {
        final int MESSAGES = 100;
        final int CAPACITY = 64;

        final JCQueue queue = createQueue("testBackPressure", CAPACITY);

        // pump more msgs than Q size & verify msg count is as expexted
        for (int i = 0; i < MESSAGES; i++) {
            if (i >= CAPACITY) {
                Assert.assertFalse(queue.tryPublish(i));
            } else {
                Assert.assertTrue(queue.tryPublish(i));
            }
        }
        Assert.assertEquals(CAPACITY, queue.size());

        Assert.assertEquals(0, queue.getOverflowCount());

        // drain 1 element and ensure BP is relieved (i.e tryPublish() succeeds)
        final MutableLong consumeCount = new MutableLong(0);
        queue.consume(new TestConsumer(), () -> consumeCount.increment() <= 1);
        Assert.assertEquals(CAPACITY - 1, queue.size());
        Assert.assertTrue(queue.tryPublish(0));
        
        queue.close();
    }

    private static class TestConsumer implements Consumer {
        int lastMsg = 0;

        @Override
        public void accept(Object o) {
            Integer i = (Integer) o;
            Assert.assertEquals(lastMsg++, i.intValue());
            System.err.println(i);
        }

        @Override
        public void flush() throws InterruptedException { }
    }

}
