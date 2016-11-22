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
package org.apache.storm.scheduler.blacklist;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

public class TestCircularBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(TestCircularBuffer.class);

    @Test
    public void TestOverwrite() {
        CircularBuffer circularQueue = new CircularBuffer(14);
        for (int i = 0; i < 18; i++) {
            circularQueue.add(i);
        }
        int[] array = new int[circularQueue.size()];
        int index = 0;
        Iterator<Serializable> it = circularQueue.iterator();
        while (it.hasNext()) {
            array[index++] = (int) it.next();
        }
        int[] correctArray = {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
        Assert.assertArrayEquals("CircularBuffer does not match", correctArray, array);
    }
}
