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

package org.apache.storm.windowing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.storm.generated.GlobalStreamId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link WaterMarkEventGenerator}
 */
public class WaterMarkEventGeneratorTest {
    WaterMarkEventGenerator<Integer> waterMarkEventGenerator;
    WindowManager<Integer> windowManager;
    List<Event<Integer>> eventList = new ArrayList<>();

    private GlobalStreamId streamId(String component) {
        return new GlobalStreamId(component, "default");
    }

    @BeforeEach
    public void setUp() {
        windowManager = new WindowManager<Integer>(null) {
            @Override
            public void add(Event<Integer> event) {
                eventList.add(event);
            }
        };
        // set watermark interval to a high value and trigger manually to fix timing issues
        waterMarkEventGenerator = new WaterMarkEventGenerator<>(windowManager, 100000, 5,
                                                                Collections.singleton(streamId("s1")));
        waterMarkEventGenerator.start();
    }

    @AfterEach
    public void tearDown() {
        waterMarkEventGenerator.shutdown();
    }

    @Test
    public void testTrackSingleStream() {
        waterMarkEventGenerator.track(streamId("s1"), 100);
        waterMarkEventGenerator.track(streamId("s1"), 110);
        waterMarkEventGenerator.run();
        assertTrue(eventList.get(0).isWatermark());
        assertEquals(105, eventList.get(0).getTimestamp());
    }

    @Test
    public void testTrackSingleStreamOutOfOrder() {
        waterMarkEventGenerator.track(streamId("s1"), 100);
        waterMarkEventGenerator.track(streamId("s1"), 110);
        waterMarkEventGenerator.track(streamId("s1"), 104);
        waterMarkEventGenerator.run();
        assertTrue(eventList.get(0).isWatermark());
        assertEquals(105, eventList.get(0).getTimestamp());
    }

    @Test
    public void testTrackTwoStreams() {
        Set<GlobalStreamId> streams = new HashSet<>();
        streams.add(streamId("s1"));
        streams.add(streamId("s2"));
        waterMarkEventGenerator = new WaterMarkEventGenerator<>(windowManager, 100000, 5, streams);

        waterMarkEventGenerator.track(streamId("s1"), 100);
        waterMarkEventGenerator.track(streamId("s1"), 110);
        waterMarkEventGenerator.run();
        assertTrue(eventList.isEmpty());
        waterMarkEventGenerator.track(streamId("s2"), 95);
        waterMarkEventGenerator.track(streamId("s2"), 98);
        waterMarkEventGenerator.run();
        assertTrue(eventList.get(0).isWatermark());
        assertEquals(93, eventList.get(0).getTimestamp());
    }

    @Test
    public void testNoEvents() {
        waterMarkEventGenerator.run();
        assertTrue(eventList.isEmpty());
    }

    @Test
    public void testLateEvent() {
        assertTrue(waterMarkEventGenerator.track(streamId("s1"), 100));
        assertTrue(waterMarkEventGenerator.track(streamId("s1"), 110));
        waterMarkEventGenerator.run();
        assertTrue(eventList.get(0).isWatermark());
        assertEquals(105, eventList.get(0).getTimestamp());
        eventList.clear();
        assertTrue(waterMarkEventGenerator.track(streamId("s1"), 105));
        assertTrue(waterMarkEventGenerator.track(streamId("s1"), 106));
        assertTrue(waterMarkEventGenerator.track(streamId("s1"), 115));
        assertFalse(waterMarkEventGenerator.track(streamId("s1"), 104));
        waterMarkEventGenerator.run();
        assertTrue(eventList.get(0).isWatermark());
        assertEquals(110, eventList.get(0).getTimestamp());
    }
}
