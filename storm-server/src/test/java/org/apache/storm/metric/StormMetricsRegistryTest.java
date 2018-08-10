/*
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

package org.apache.storm.metric;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;
import static org.junit.jupiter.api.Assertions.*;

class StormMetricsRegistryTest {
    private static final Logger LOG = LoggerFactory.getLogger(StormMetricsRegistryTest.class);

    private static final String OUTER_METER = "outerMeter";
    private static final String INNER_SET = "innerSet";
    private static final String OUTER_TIMER = "outerTimer";
    private static final String INNER_METER = "innerMeter";
    private static final String INNER_TIMER = "innerTimer";
    private static final MetricSet OUTER = newMetricSetInstance();

    @Test
    void registerMetricSet() {
        Meter existingInnerMeter = StormMetricsRegistry.registerMeter(name(INNER_SET, INNER_METER));

        LOG.info("register outer set");
        StormMetricsRegistry.registerMetricSet(OUTER);
        assertSame(OUTER.getMetrics().get(OUTER_TIMER), StormMetricsRegistry.REGISTRY.getMetrics().get(OUTER_TIMER));
        assertSame(OUTER.getMetrics().get(OUTER_METER), StormMetricsRegistry.REGISTRY.getMetrics().get(OUTER_METER));
        assertSame(((MetricSet) OUTER.getMetrics().get(INNER_SET)).getMetrics().get(INNER_TIMER),
            StormMetricsRegistry.REGISTRY.getMetrics().get(name(INNER_SET, INNER_TIMER)));

        assertNotSame(((MetricSet) OUTER.getMetrics().get(INNER_SET)).getMetrics().get(INNER_METER),
            StormMetricsRegistry.REGISTRY.getMetrics().get(name(INNER_SET, INNER_METER)));
        assertSame(existingInnerMeter, StormMetricsRegistry.REGISTRY.getMetrics().get(name(INNER_SET, INNER_METER)));

        //Ensure idempotency
        LOG.info("twice register outer set");
        MetricSet newOuter = newMetricSetInstance();
        StormMetricsRegistry.registerMetricSet(newOuter);
        assertSame(OUTER.getMetrics().get(OUTER_TIMER), StormMetricsRegistry.REGISTRY.getMetrics().get(OUTER_TIMER));
        assertSame(OUTER.getMetrics().get(OUTER_METER), StormMetricsRegistry.REGISTRY.getMetrics().get(OUTER_METER));
        assertSame(((MetricSet) OUTER.getMetrics().get(INNER_SET)).getMetrics().get(INNER_TIMER),
            StormMetricsRegistry.REGISTRY.getMetrics().get(name(INNER_SET, INNER_TIMER)));
        assertSame(existingInnerMeter, StormMetricsRegistry.REGISTRY.getMetrics().get(name(INNER_SET, INNER_METER)));

        LOG.info("name collision");
        assertThrows(IllegalArgumentException.class, () -> StormMetricsRegistry.registerGauge(name(INNER_SET, INNER_METER), () -> 0));
    }

    @Test
    void unregisterMetricSet() {
        StormMetricsRegistry.registerMetricSet(OUTER);
        StormMetricsRegistry.unregisterMetricSet(OUTER);
        assertTrue(StormMetricsRegistry.REGISTRY.getMetrics().isEmpty());

    }

    private static MetricSet newMetricSetInstance() {
        return new MetricSet() {
            private final MetricSet inner = new MetricSet() {
                private final Map<String, Metric> map = new HashMap<>();

                {
                    map.put(INNER_METER, new Meter());
                    map.put(INNER_TIMER, new Timer());
                }

                @Override
                public Map<String, Metric> getMetrics() {
                    return map;
                }
            };
            private final Map<String, Metric> outerMap = new HashMap<>();

            {
                outerMap.put(OUTER_METER, new Meter());
                outerMap.put(INNER_SET, inner);
                outerMap.put(OUTER_TIMER, new Timer());
            }

            @Override
            public Map<String, Metric> getMetrics() {
                return outerMap;
            }
        };
    }
}