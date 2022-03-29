/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.trident;

import java.util.concurrent.TimeUnit;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.windowing.InMemoryWindowsStore;
import org.apache.storm.trident.windowing.config.SlidingCountWindow;
import org.apache.storm.trident.windowing.config.SlidingDurationWindow;
import org.apache.storm.trident.windowing.config.TumblingCountWindow;
import org.apache.storm.trident.windowing.config.TumblingDurationWindow;
import org.apache.storm.trident.windowing.strategy.SlidingCountWindowStrategy;
import org.apache.storm.trident.windowing.strategy.SlidingDurationWindowStrategy;
import org.apache.storm.trident.windowing.strategy.TumblingCountWindowStrategy;
import org.apache.storm.trident.windowing.strategy.TumblingDurationWindowStrategy;
import org.apache.storm.trident.windowing.strategy.WindowStrategy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class TridentWindowingTest {

    @Test
    public void testWindowStrategyInstances() {

        WindowStrategy<Object> tumblingCountStrategy = TumblingCountWindow.of(10).getWindowStrategy();
        assertTrue(tumblingCountStrategy instanceof TumblingCountWindowStrategy);

        WindowStrategy<Object> slidingCountStrategy = SlidingCountWindow.of(100, 10).getWindowStrategy();
        assertTrue(slidingCountStrategy instanceof SlidingCountWindowStrategy);

        WindowStrategy<Object> tumblingDurationStrategy = TumblingDurationWindow.of(
            new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS))
                                                                                .getWindowStrategy();
        assertTrue(tumblingDurationStrategy instanceof TumblingDurationWindowStrategy);

        WindowStrategy<Object> slidingDurationStrategy = SlidingDurationWindow.of(
            new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS),
            new BaseWindowedBolt.Duration(2, TimeUnit.SECONDS))
                                                                              .getWindowStrategy();
        assertTrue(slidingDurationStrategy instanceof SlidingDurationWindowStrategy);
    }

    @Test
    public void testWindowConfig() {
        int windowLength = 9;
        TumblingCountWindow tumblingCountWindow = TumblingCountWindow.of(windowLength);
        assertTrue(tumblingCountWindow.getWindowLength() == windowLength);
        assertTrue(tumblingCountWindow.getSlidingLength() == windowLength);

        windowLength = 10;
        int slidingLength = 2;
        SlidingCountWindow slidingCountWindow = SlidingCountWindow.of(10, 2);
        assertTrue(slidingCountWindow.getWindowLength() == windowLength);
        assertTrue(slidingCountWindow.getSlidingLength() == slidingLength);

        windowLength = 20;
        TumblingDurationWindow tumblingDurationWindow =
            TumblingDurationWindow.of(new BaseWindowedBolt.Duration(windowLength, TimeUnit.SECONDS));
        assertTrue(tumblingDurationWindow.getWindowLength() == windowLength * 1000);
        assertTrue(tumblingDurationWindow.getSlidingLength() == windowLength * 1000);

        windowLength = 50;
        slidingLength = 10;
        SlidingDurationWindow slidingDurationWindow =
            SlidingDurationWindow.of(new BaseWindowedBolt.Duration(windowLength, TimeUnit.SECONDS),
                                     new BaseWindowedBolt.Duration(slidingLength, TimeUnit.SECONDS));
        assertTrue(slidingDurationWindow.getWindowLength() == windowLength * 1000);
        assertTrue(slidingDurationWindow.getSlidingLength() == slidingLength * 1000);
    }

    @Test
    public void testInMemoryWindowStore() {
        InMemoryWindowsStore store = new InMemoryWindowsStore();
        String keyPrefix = "key";
        String valuePrefix = "valuePrefix";

        int ct = 10;
        for (int i = 0; i < ct; i++) {
            store.put(keyPrefix + i, valuePrefix + i);
        }

        for (int i = 0; i < ct; i++) {
            assertTrue((valuePrefix + i).equals(store.get(keyPrefix + i)));
        }

        store.remove(keyPrefix + 1);
        assertNull(store.get(keyPrefix + 1));

    }

}
