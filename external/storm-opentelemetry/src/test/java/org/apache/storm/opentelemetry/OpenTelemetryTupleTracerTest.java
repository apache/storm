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

package org.apache.storm.opentelemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class OpenTelemetryTupleTracerTest {

    private static final int SPOUT_TASK = 1;

    @Test
    public void testLooksForAnSdkAtMostOnceASecondUntilOneIsRegistered() {
        OpenTelemetryTupleTracer tracer = preparedTracer();
        try (SimulatedTime ignored = new SimulatedTime();
             MockedStatic<GlobalOpenTelemetry> global = mockStatic(GlobalOpenTelemetry.class);
             OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(SdkTracerProvider.builder().build()).build()) {
            global.when(GlobalOpenTelemetry::isSet).thenReturn(false);

            assertNull(tracer.spoutEmit(SPOUT_TASK, Utils.DEFAULT_STREAM_ID));
            assertNull(tracer.spoutEmit(SPOUT_TASK, Utils.DEFAULT_STREAM_ID));
            Time.advanceTime(999);
            assertNull(tracer.spoutEmit(SPOUT_TASK, Utils.DEFAULT_STREAM_ID));
            global.verify(GlobalOpenTelemetry::isSet, times(1));

            global.when(GlobalOpenTelemetry::isSet).thenReturn(true);
            global.when(GlobalOpenTelemetry::get).thenReturn(sdk);
            assertNull(tracer.spoutEmit(SPOUT_TASK, Utils.DEFAULT_STREAM_ID), "registered within the same second");
            Time.advanceTime(1);
            assertNotNull(tracer.spoutEmit(SPOUT_TASK, Utils.DEFAULT_STREAM_ID));
            assertNotNull(tracer.spoutEmit(SPOUT_TASK, Utils.DEFAULT_STREAM_ID));
            global.verify(GlobalOpenTelemetry::isSet, times(2));
        }
    }

    private static OpenTelemetryTupleTracer preparedTracer() {
        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        when(context.getStormId()).thenReturn("topology-1-0");
        when(context.getThisWorkerPort()).thenReturn(6700);
        when(context.getThisWorkerTasks()).thenReturn(List.of(SPOUT_TASK));
        when(context.getComponentId(SPOUT_TASK)).thenReturn("spout");
        OpenTelemetryTupleTracer tracer = new OpenTelemetryTupleTracer();
        tracer.prepare(Map.of(Config.TOPOLOGY_NAME, "topology"), context);
        return tracer;
    }
}
