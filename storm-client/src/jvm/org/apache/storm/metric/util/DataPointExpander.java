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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPointExpander implements Serializable {
    public static final Logger LOG = LoggerFactory.getLogger(DataPointExpander.class);

    private final boolean expandMapType;
    private final String metricNameSeparator;

    public DataPointExpander(boolean expandMapType, String metricNameSeparator) {
        this.expandMapType = expandMapType;
        this.metricNameSeparator = metricNameSeparator;
    }

    public Collection<IMetricsConsumer.DataPoint> expandDataPoints(Collection<IMetricsConsumer.DataPoint> dataPoints) {
        if (!expandMapType) {
            return dataPoints;
        }

        List<IMetricsConsumer.DataPoint> expandedDataPoints = new ArrayList<>();

        for (IMetricsConsumer.DataPoint dataPoint : dataPoints) {
            expandedDataPoints.addAll(expandDataPoint(dataPoint));
        }

        return expandedDataPoints;
    }

    public Collection<IMetricsConsumer.DataPoint> expandDataPoint(IMetricsConsumer.DataPoint dataPoint) {
        if (!expandMapType) {
            return Collections.singletonList(dataPoint);
        }

        List<IMetricsConsumer.DataPoint> dataPoints = new ArrayList<>();

        if (dataPoint.value == null) {
            LOG.warn("Data point with name {} is null. Discarding.", dataPoint.name);
        } else if (dataPoint.value instanceof Map) {
            Map<Object, Object> dataMap = (Map<Object, Object>) dataPoint.value;

            for (Map.Entry<Object, Object> entry : dataMap.entrySet()) {
                String expandedDataPointName = dataPoint.name + metricNameSeparator + String.valueOf(entry.getKey());
                dataPoints.add(new IMetricsConsumer.DataPoint(expandedDataPointName, entry.getValue()));
            }
        } else {
            dataPoints.add(dataPoint);
        }

        return dataPoints;
    }

}
