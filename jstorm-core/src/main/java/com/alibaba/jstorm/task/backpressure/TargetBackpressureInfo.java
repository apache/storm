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
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.jstorm.task.master.TopoMasterCtrlEvent.EventType;

public class TargetBackpressureInfo implements Serializable {
    private static final long serialVersionUID = -1829897435773792484L;
    
    private Set<Integer> tasks;
    
    private EventType backpressureStatus;
    private int flowCtrlTime;
    private long timeStamp;

    public TargetBackpressureInfo() {
        this.tasks = new TreeSet<Integer>();
        this.backpressureStatus = EventType.defaultType;
        this.flowCtrlTime = -1;
        this.timeStamp = 0l;
    }

    public TargetBackpressureInfo(EventType backpressureStatus, int flowCtrlTime, long time) {
        this.tasks = new TreeSet<Integer>();
        this.backpressureStatus = backpressureStatus;
        this.flowCtrlTime = flowCtrlTime;
        this.timeStamp = time;
    }

    public Set<Integer> getTasks() {
        return tasks;
    }

    public void setBackpressureStatus(EventType status) {
        this.backpressureStatus = status;
    }

    public EventType getBackpressureStatus() {
        return this.backpressureStatus;
    }

    public void setTimeStamp(long time) {
        this.timeStamp = time;
    }

    public long getTimeStamp() {
        return this.timeStamp;
    }

    public int getFlowCtrlTime() {
        return this.flowCtrlTime;
    }

    public void setFlowCtrlTime(int time) {
        this.flowCtrlTime = time;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
