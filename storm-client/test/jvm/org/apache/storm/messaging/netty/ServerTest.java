/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.messaging.netty;

import java.util.Map;
import org.apache.storm.messaging.DeserializingConnectionCallback;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerTest {

    @Test
    public void testGetStateReportsDeserializationFailures() {
        DeserializingConnectionCallback cb = mock(DeserializingConnectionCallback.class);
        when(cb.getAndResetDeserializationFailures()).thenReturn(7L, 0L);
        Server server = new Server(Utils.readStormConfig(), 0, cb, null);
        try {
            Object state = server.getState();
            assertTrue(state instanceof Map);
            assertEquals(7L, ((Map<?, ?>) state).get("deserializationFailures"));

            // the key stays present once the count has been read
            assertEquals(0L, ((Map<?, ?>) server.getState()).get("deserializationFailures"));
        } finally {
            server.close();
        }
    }
}
