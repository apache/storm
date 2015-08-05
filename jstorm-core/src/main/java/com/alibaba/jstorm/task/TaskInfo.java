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
package com.alibaba.jstorm.task;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * /storm-zk-root/tasks/{topologyid}/{taskid} data
 */
public class TaskInfo implements Serializable {

    private static final long serialVersionUID = 5625165079055837777L;
    private String componentId;
    private String componentType;

    public TaskInfo(String componentId, String componentType) {
        this.componentId = componentId;
        this.componentType = componentType;
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public String getComponentType() {
        return componentType;
    }

    public void setComponentType(String componentType) {
        this.componentType = componentType;
    }

    @Override
    public boolean equals(Object assignment) {
        if (assignment instanceof TaskInfo
                && ((TaskInfo) assignment).getComponentId().equals(
                        getComponentId())
                && ((TaskInfo) assignment).getComponentType().equals(
                        componentType)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.getComponentId().hashCode()
                + this.getComponentType().hashCode();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
