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

package org.apache.storm.messaging;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.task.GeneralTopologyContext;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeserializingConnectionCallbackTest {
    private static final byte[] messageBytes = new byte[3];
    private static TaskMessage message;

    @Before
    public void setUp() throws Exception {
        // Setup a test message
        message = mock(TaskMessage.class);
        when(message.task()).thenReturn(456); // destination taskId
        when(message.message()).thenReturn(messageBytes);
    }


    @Test
    public void testUpdateMetricsConfigOff() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_SERIALIZED_MESSAGE_SIZE_METRICS, Boolean.FALSE);
        DeserializingConnectionCallback withoutMetrics =
            new DeserializingConnectionCallback(config, mock(GeneralTopologyContext.class), mock(
                WorkerState.ILocalTransferCallback.class));

        // Metrics are off, verify null
        assertNull(withoutMetrics.getValueAndReset());

        // Add our messages and verify no metrics are recorded  
        withoutMetrics.updateMetrics(123, message);
        assertNull(withoutMetrics.getValueAndReset());
    }

    @Test
    public void testUpdateMetricsConfigOn() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_SERIALIZED_MESSAGE_SIZE_METRICS, Boolean.TRUE);
        DeserializingConnectionCallback withMetrics =
            new DeserializingConnectionCallback(config, mock(GeneralTopologyContext.class), mock(
                WorkerState.ILocalTransferCallback.class));

        // Starting empty
        Object metrics = withMetrics.getValueAndReset();
        assertTrue(metrics instanceof Map);
        assertTrue(((Map) metrics).isEmpty());

        // Add messages
        withMetrics.updateMetrics(123, message);
        withMetrics.updateMetrics(123, message);

        // Verify recorded messages size metrics 
        metrics = withMetrics.getValueAndReset();
        assertTrue(metrics instanceof Map);
        assertEquals(6L, ((Map) metrics).get("123-456"));
    }
}
