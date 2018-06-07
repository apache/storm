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

package org.apache.storm.metric.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataPointExpanderTest {

    @Test
    public void testExpandDataPointWithExpandDisabled() {
        DataPointExpander populator = new DataPointExpander(false, ".");
        Map<String, Object> value = getDummyMetricMapValue();
        IMetricsConsumer.DataPoint point = new IMetricsConsumer.DataPoint("test", value);

        Collection<IMetricsConsumer.DataPoint> expandedDataPoints = populator.expandDataPoint(point);
        assertEquals(1, expandedDataPoints.size());
        assertEquals(point, expandedDataPoints.iterator().next());
    }

    @Test
    public void testExpandDataPointsWithExpandDisabled() {
        DataPointExpander populator = new DataPointExpander(false, ".");
        Map<String, Object> value = getDummyMetricMapValue();
        IMetricsConsumer.DataPoint point = new IMetricsConsumer.DataPoint("test", value);

        Collection<IMetricsConsumer.DataPoint> expandedDataPoints =
            populator.expandDataPoints(Collections.singletonList(point));
        assertEquals(1, expandedDataPoints.size());
        assertEquals(point, expandedDataPoints.iterator().next());
    }

    @Test
    public void testExpandDataPointWithVariousKindOfMetrics() {
        DataPointExpander populator = new DataPointExpander(true, ".");

        IMetricsConsumer.DataPoint point = new IMetricsConsumer.DataPoint("point", getDummyMetricMapValue());

        Collection<IMetricsConsumer.DataPoint> expandedDataPoints =
            populator.expandDataPoints(Collections.singletonList(point));
        assertEquals(4, expandedDataPoints.size());
        assertTrue(expandedDataPoints.contains(new IMetricsConsumer.DataPoint("point.a", 1.0)));
        assertTrue(expandedDataPoints.contains(new IMetricsConsumer.DataPoint("point.b", 2.5)));
        assertTrue(expandedDataPoints.contains(new IMetricsConsumer.DataPoint("point.c", null)));
        assertTrue(expandedDataPoints.contains(new IMetricsConsumer.DataPoint("point.d", "hello")));
    }

    @Test
    public void testExpandDataPointsWithVariousKindOfMetrics() {
        DataPointExpander populator = new DataPointExpander(true, ":");

        IMetricsConsumer.DataPoint point1 = new IMetricsConsumer.DataPoint("point1", 2.5);
        IMetricsConsumer.DataPoint point2 = new IMetricsConsumer.DataPoint("point2", getDummyMetricMapValue());

        Collection<IMetricsConsumer.DataPoint> expandedDataPoints =
            populator.expandDataPoints(Lists.newArrayList(point1, point2));
        // 1 for point1, 4 for point2
        assertEquals(5, expandedDataPoints.size());
        assertTrue(expandedDataPoints.contains(new IMetricsConsumer.DataPoint("point1", 2.5)));
        assertTrue(expandedDataPoints.contains(new IMetricsConsumer.DataPoint("point2:a", 1.0)));
        assertTrue(expandedDataPoints.contains(new IMetricsConsumer.DataPoint("point2:b", 2.5)));
        assertTrue(expandedDataPoints.contains(new IMetricsConsumer.DataPoint("point2:c", null)));
        assertTrue(expandedDataPoints.contains(new IMetricsConsumer.DataPoint("point2:d", "hello")));
    }

    @Test
    public void testExpandDataPointWithNullValueMetric() {
        DataPointExpander populator = new DataPointExpander(true, ".");

        IMetricsConsumer.DataPoint point = new IMetricsConsumer.DataPoint("point", null);
        Collection<IMetricsConsumer.DataPoint> expandedDataPoints = populator.expandDataPoint(point);

        assertEquals(0, expandedDataPoints.size());
    }

    private Map<String, Object> getDummyMetricMapValue() {
        Map<String, Object> value = new HashMap<>();
        value.put("a", 1.0);
        value.put("b", 2.5);
        value.put("c", null);
        value.put("d", "hello"); // yes it's just a test purpose
        return value;
    }

}
