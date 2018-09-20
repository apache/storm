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

package org.apache.storm.executor.error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.Config;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReportErrorTest {

    @Test
    public void testReport() {
        final String topo = "topology";
        final String comp = "component";
        final Long port = new Long(8080);
        final AtomicLong errorCount = new AtomicLong(0l);

        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        when(context.getThisWorkerPort()).thenReturn(port.intValue());

        IStormClusterState state = mock(IStormClusterState.class);
        doAnswer((invocation) -> errorCount.incrementAndGet())
            .when(state).reportError(eq(topo), eq(comp), any(String.class), eq(port), any(Throwable.class));
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS, 10);
        conf.put(Config.TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL, 4);

        try (SimulatedTime t = new SimulatedTime()) {
            ReportError report = new ReportError(conf, state, topo, comp, context);
            report.report(new RuntimeException("ERROR-1"));
            assertEquals(1, errorCount.get());
            report.report(new RuntimeException("ERROR-2"));
            assertEquals(2, errorCount.get());
            report.report(new RuntimeException("ERROR-3"));
            assertEquals(3, errorCount.get());
            report.report(new RuntimeException("ERROR-4"));
            assertEquals(4, errorCount.get());
            //Too fast not reported
            report.report(new RuntimeException("ERROR-5"));
            assertEquals(4, errorCount.get());
            Time.advanceTime(9000);
            report.report(new RuntimeException("ERROR-6"));
            assertEquals(4, errorCount.get());
            Time.advanceTime(2000);
            report.report(new RuntimeException("ERROR-7"));
            assertEquals(5, errorCount.get());
        }
    }
}
