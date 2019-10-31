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

package org.apache.storm.streams;

import java.io.Serializable;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.tuple.Fields;

abstract class GroupingInfo implements Serializable {
    private final Fields fields;

    private GroupingInfo() {
        this(null);
    }

    private GroupingInfo(Fields fields) {
        this.fields = fields;
    }

    public static GroupingInfo shuffle() {
        return new GroupingInfo() {
            @Override
            public void declareGrouping(BoltDeclarer declarer, String componentId, String streamId, Fields fields) {
                declarer.shuffleGrouping(componentId, streamId);
            }
        };
    }

    public static GroupingInfo fields(Fields fields) {
        return new GroupingInfo(fields) {
            @Override
            public void declareGrouping(BoltDeclarer declarer, String componentId, String streamId, Fields fields) {
                declarer.fieldsGrouping(componentId, streamId, fields);
            }
        };
    }

    public static GroupingInfo global() {
        return new GroupingInfo() {
            @Override
            public void declareGrouping(BoltDeclarer declarer, String componentId, String streamId, Fields fields) {
                declarer.globalGrouping(componentId, streamId);
            }
        };
    }

    public static GroupingInfo all() {
        return new GroupingInfo() {
            @Override
            public void declareGrouping(BoltDeclarer declarer, String componentId, String streamId, Fields fields) {
                declarer.allGrouping(componentId, streamId);
            }
        };
    }

    public abstract void declareGrouping(BoltDeclarer declarer, String componentId, String streamId, Fields fields);

    public Fields getFields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GroupingInfo that = (GroupingInfo) o;

        return fields != null ? fields.equals(that.fields) : that.fields == null;

    }

    @Override
    public int hashCode() {
        return fields != null ? fields.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "GroupingInfo{"
                + "fields=" + fields
                + '}';
    }
}
