/**
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
package com.alibaba.jstorm.task.backpressure;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import com.alibaba.jstorm.task.master.TopoMasterCtrlEvent.EventType;

public class SourceBackpressureInfo implements Serializable {
    private static final long serialVersionUID = -8213491092461721871L;

    // source tasks under backpressure
    private Set<Integer> tasks;

    // target tasks which has sent request to source task
    // Map<componentId, source task backpressure info>
    private Map<String, TargetBackpressureInfo> targetTasks;

    public SourceBackpressureInfo() {
        this.tasks = new TreeSet<Integer>();
        this.targetTasks = new HashMap<String, TargetBackpressureInfo>();
    }

    public Set<Integer> getTasks() {
        return tasks;
    }

    public Map<String, TargetBackpressureInfo> getTargetTasks() {
        return targetTasks;
    }

    public long getLastestTimeStamp() {
        long ret = 0;

        for (Entry<String, TargetBackpressureInfo> entry : targetTasks.entrySet()) {
            TargetBackpressureInfo info = entry.getValue();
            if (info.getTimeStamp() > ret) {
                ret = info.getTimeStamp();
            }
        }
        return ret;
    }

    public EventType getLastestBackpressureEvent() {
        EventType ret = null;
        long timeStamp = 0;

        for (Entry<String, TargetBackpressureInfo> entry : targetTasks.entrySet()) {
            TargetBackpressureInfo info = entry.getValue();
            if (info.getTimeStamp() > timeStamp) {
                timeStamp = info.getTimeStamp();
                ret = info.getBackpressureStatus();
            }
        }

        return ret;
    }

    public int getMaxFlowCtrlTime() {
        int ret = 0;

        for (Entry<String, TargetBackpressureInfo> entry : targetTasks.entrySet()) {
            TargetBackpressureInfo info = entry.getValue();
            if (info.getFlowCtrlTime() > ret) {
                ret = info.getFlowCtrlTime();
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
