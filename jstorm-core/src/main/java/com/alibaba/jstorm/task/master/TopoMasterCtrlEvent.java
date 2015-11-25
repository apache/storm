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
package com.alibaba.jstorm.task.master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Definition of control event which is used for the control purpose in
 * topology, e.g. back pressure
 * 
 * @author Basti Liu 
 */

public class TopoMasterCtrlEvent implements Serializable {

    private static final long serialVersionUID = 5929540385279089750L;

    public enum EventType {
        startBackpressure, stopBackpressure, syncBackpressureState, updateBackpressureConfig, defaultType
    }

    private EventType eventType;
    private List<Object> eventValue;

    public TopoMasterCtrlEvent() {
        eventType = EventType.defaultType;
        eventValue = null;
    }

    public TopoMasterCtrlEvent(EventType type, List<Object> value) {
        this.eventType = type;
        this.eventValue = value;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType type) {
        this.eventType = type;
    }

    public List<Object> getEventValue() {
        return eventValue;
    }

    public void setEventValue(List<Object> value) {
        this.eventValue = value;
    }

    public void addEventValue(Object value) {
        if (eventValue == null) {
            eventValue = new ArrayList<Object>();
        }

        eventValue.add(value);
    }
}