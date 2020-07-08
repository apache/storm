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

import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.storm.Config;
import org.apache.storm.task.WorkerTopologyContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BoltExecutorMetricsTest extends TestCase {

    @Test
    public void testCounts() {
        WorkerTopologyContext workerTopologyContext = Mockito.mock(WorkerTopologyContext.class);
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.NUM_STAT_BUCKETS, 20);
        topoConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.05);
        BoltExecutorMetrics boltExecutorMetrics = new BoltExecutorMetrics(workerTopologyContext, new StormMetricRegistry(), topoConf);

        validateExecutorMetrics(boltExecutorMetrics);

        // test fail
        boltExecutorMetrics.boltFailedTuple("component1", "stream1", 1);
        Long stream1Count = boltExecutorMetrics.getFailTimeCounts().get("600").get("[component1, stream1]");
        Assert.assertEquals(new Long(20L),stream1Count);
        Long stream2Count = boltExecutorMetrics.getFailTimeCounts().get("600").get("[component1, stream2]");
        Assert.assertNull(stream2Count);

        // test ack
        boltExecutorMetrics.boltAckedTuple("component1", "stream1", 1, 10);
        boltExecutorMetrics.boltAckedTuple("component1", "stream1", 1, 20);
        boltExecutorMetrics.boltAckedTuple("component1", "stream2", 2, 222);
        stream1Count = boltExecutorMetrics.getAckTimeCounts().get("600").get("[component1, stream1]");
        Assert.assertEquals(new Long(40L), stream1Count);
        stream2Count = boltExecutorMetrics.getAckTimeCounts().get("600").get("[component1, stream2]");
        Assert.assertEquals(new Long(20L), stream2Count);
        Double stream1Latency = boltExecutorMetrics.getProcessLatencyTimeCounts().get("600").get("[component1, stream1]");
        Assert.assertEquals(15.0, stream1Latency, 0.001);
        Double stream2Latency = boltExecutorMetrics.getProcessLatencyTimeCounts().get("600").get("[component1, stream2]");
        Assert.assertEquals(222.0, stream2Latency, 0.001);

        // test execute
        boltExecutorMetrics.boltExecuteTuple("component1", "stream1", 1, 10);
        boltExecutorMetrics.boltExecuteTuple("component1", "stream2", 1, 20);
        boltExecutorMetrics.boltExecuteTuple("component1", "stream2", 2, 30);
        stream1Count = boltExecutorMetrics.getExecutedTimeCounts().get("600").get("[component1, stream1]");
        Assert.assertEquals(new Long(20L), stream1Count);
        stream2Count = boltExecutorMetrics.getExecutedTimeCounts().get("600").get("[component1, stream2]");
        Assert.assertEquals(new Long(40L), stream2Count);

        // execute latency
        stream1Latency = boltExecutorMetrics.getExecuteLatencyTimeCounts().get("600").get("[component1, stream1]");
        Assert.assertEquals(10.0, stream1Latency, 0.001);
        stream2Latency = boltExecutorMetrics.getExecuteLatencyTimeCounts().get("600").get("[component1, stream2]");
        Assert.assertEquals(25.0, stream2Latency, 0.001);
    }

    private void validateExecutorMetrics(ExecutorMetrics executorMetrics) {
        // emitted
        executorMetrics.emittedTuple("component1", "stream1", 1);
        executorMetrics.emittedTuple("component1", "stream2", 2);
        executorMetrics.emittedTuple("component1", "stream1", 1);
        Long stream1Count = executorMetrics.getEmitTimeCounts().get("600").get("stream1");
        Assert.assertEquals(new Long(40L), stream1Count);
        Long stream2Count = executorMetrics.getEmitTimeCounts().get("600").get("stream2");
        Assert.assertEquals(new Long(20L), stream2Count);

        // transferred
        executorMetrics.transferredTuples("component1", "stream1", 1, 100);
        executorMetrics.transferredTuples("component1", "stream2", 2, 30);
        executorMetrics.transferredTuples("component1", "stream1", 1, 200);
        stream1Count = executorMetrics.getTransferTimeCounts().get("600").get("stream1");
        Assert.assertEquals(new Long(6000L), stream1Count);
        stream2Count = executorMetrics.getTransferTimeCounts().get("600").get("stream2");
        Assert.assertEquals(new Long(600L), stream2Count);
    }
}
