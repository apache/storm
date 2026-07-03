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
package org.apache.storm;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ConstantsTest {

    @Test
    public void testEveryWhitelistedStreamIsControl() {
        for (String id : Constants.SYSTEM_CONTROL_STREAM_IDS) {
            // fresh instance: the fast-reject gates must not depend on identity or a cached hashCode
            assertTrue(Constants.isControlStreamId(new String(id.toCharArray())), id);
        }
    }

    @Test
    public void testHighVolumeSystemStreamsAreNotControl() {
        // volume-proportional to the data plane: must stay on the data path (see SYSTEM_CONTROL_STREAM_IDS javadoc)
        String[] dataPlaneSystemStreams = {
            "__ack_init", "__ack_ack", "__ack_fail", "__ack_reset_timeout",
            "__metrics", "__system", "__eventlog", "__heartbeat"
        };
        for (String id : dataPlaneSystemStreams) {
            assertFalse(Constants.isControlStreamId(id), id);
        }
    }

    @Test
    public void testUserStreamsAreNotControl() {
        // includes ids whose length collides with a whitelisted id, to exercise the prefix/char gates
        String[] userStreams = { "default", "s1", "stream", "__x", "_tick", "___tick", "abcdef", "abcdefghij" };
        for (String id : userStreams) {
            assertFalse(Constants.isControlStreamId(id), id);
        }
    }

    @Test
    public void testDegenerateStreamIdsAreNotControl() {
        assertFalse(Constants.isControlStreamId(null));
        assertFalse(Constants.isControlStreamId(""));
        assertFalse(Constants.isControlStreamId("__"));
        StringBuilder longId = new StringBuilder("__");
        for (int i = 0; i < 100; i++) {
            longId.append('t');
        }
        assertFalse(Constants.isControlStreamId(longId.toString()));
    }
}
