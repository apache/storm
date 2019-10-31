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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.opentsdb.bolt;

import java.util.Map;

import org.apache.storm.opentsdb.OpenTsdbMetricDatapoint;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts {@link org.apache.storm.tuple.ITuple} to {@link OpenTsdbMetricDatapoint}.
 */
public final class TupleOpenTsdbDatapointMapper implements ITupleOpenTsdbDatapointMapper {
    private static final Logger LOG = LoggerFactory.getLogger(TupleOpenTsdbDatapointMapper.class);

    /**
     * Default mapper which can be used when the tuple already contains fields mapping  metric, timestamp, tags and value.
     */
    public static final TupleOpenTsdbDatapointMapper DEFAULT_MAPPER =
            new TupleOpenTsdbDatapointMapper("metric", "timestamp", "tags", "value");

    private final String metricField;
    private final String timestampField;
    private final String valueField;
    private final String tagsField;

    public TupleOpenTsdbDatapointMapper(String metricField, String timestampField, String tagsField, String valueField) {
        this.metricField = metricField;
        this.timestampField = timestampField;
        this.tagsField = tagsField;
        this.valueField = valueField;
    }

    @Override
    public OpenTsdbMetricDatapoint getMetricPoint(ITuple tuple) {
        return new OpenTsdbMetricDatapoint(
                tuple.getStringByField(metricField),
                (Map<String, String>) tuple.getValueByField(tagsField),
                tuple.getLongByField(timestampField),
                (Number) tuple.getValueByField(valueField));
    }

    /**
     * Retrieve metric field name in the tuple.
     * @return metric field name in the tuple
     */
    public String getMetricField() {
        return metricField;
    }

    /**
     * Retrieve the timestamp field name in the tuple.
     * @return timestamp field name in the tuple
     */
    public String getTimestampField() {
        return timestampField;
    }

    /**
     * Retrieve the value field name in the tuple.
     * @return value field name in the tuple
     */
    public String getValueField() {
        return valueField;
    }

    /**
     * Retrieve the tags field name in the tuple.
     * @return tags field name in the tuple
     */
    public String getTagsField() {
        return tagsField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TupleOpenTsdbDatapointMapper)) {
            return false;
        }

        TupleOpenTsdbDatapointMapper that = (TupleOpenTsdbDatapointMapper) o;

        if (!metricField.equals(that.metricField)) {
            return false;
        }
        if (!timestampField.equals(that.timestampField)) {
            return false;
        }
        if (!valueField.equals(that.valueField)) {
            return false;
        }
        return tagsField.equals(that.tagsField);
    }

    @Override
    public int hashCode() {
        int result = metricField.hashCode();
        result = 31 * result + timestampField.hashCode();
        result = 31 * result + valueField.hashCode();
        result = 31 * result + tagsField.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TupleOpenTsdbDatapointMapper{"
                + "metricField='" + metricField + '\''
                + ", timestampField='" + timestampField + '\''
                + ", valueField='" + valueField + '\''
                + ", tagsField='" + tagsField + '\''
                + '}';
    }
}
