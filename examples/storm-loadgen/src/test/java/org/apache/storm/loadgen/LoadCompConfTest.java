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

package org.apache.storm.loadgen;

import org.junit.Test;

import static org.junit.Assert.*;

public class LoadCompConfTest {
    @Test
    public void scaleParallel() throws Exception {
        LoadCompConf orig = new LoadCompConf.Builder()
            .withId("SOME_SPOUT")
            .withParallelism(1)
            .withStream(new OutputStream("default", new NormalDistStats(500.0, 100.0, 300.0, 600.0), false))
            .build();
        assertEquals(500.0, orig.getAllEmittedAggregate(), 0.001);
        LoadCompConf scaled = orig.scaleParallel(2);
        //Parallelism is double
        assertEquals(2, scaled.parallelism);
        assertEquals("SOME_SPOUT", scaled.id);
        //But throughput is the same
        assertEquals(500.0, scaled.getAllEmittedAggregate(), 0.001);
    }

    @Test
    public void scaleThroughput() throws Exception {
        LoadCompConf orig = new LoadCompConf.Builder()
            .withId("SOME_SPOUT")
            .withParallelism(1)
            .withStream(new OutputStream("default", new NormalDistStats(500.0, 100.0, 300.0, 600.0), false))
            .build();
        assertEquals(500.0, orig.getAllEmittedAggregate(), 0.001);
        LoadCompConf scaled = orig.scaleThroughput(2.0);
        //Parallelism is same
        assertEquals(1, scaled.parallelism);
        assertEquals("SOME_SPOUT", scaled.id);
        //But throughput is the same
        assertEquals(1000.0, scaled.getAllEmittedAggregate(), 0.001);
    }
}