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
package com.alibaba.jstorm.task.heartbeat;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Task heartbeat, this Object will be updated to ZK timely
 * 
 * @author yannian
 * 
 */
public class TaskHeartbeat implements Serializable {

    private static final long serialVersionUID = -6369195955255963810L;
    private Integer timeSecs;
    private Integer uptimeSecs;

    public TaskHeartbeat(int timeSecs, int uptimeSecs) {
        this.timeSecs = timeSecs;
        this.uptimeSecs = uptimeSecs;
    }

    public int getTimeSecs() {
        return timeSecs;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }

    public void setTimeSecs(int timeSecs) {
        this.timeSecs = timeSecs;
    }

    public int getUptimeSecs() {
        return uptimeSecs;
    }

    public void setUptimeSecs(int uptimeSecs) {
        this.uptimeSecs = uptimeSecs;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result =
                prime * result + ((timeSecs == null) ? 0 : timeSecs.hashCode());
        result =
                prime * result
                        + ((uptimeSecs == null) ? 0 : uptimeSecs.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TaskHeartbeat other = (TaskHeartbeat) obj;
        if (timeSecs == null) {
            if (other.timeSecs != null)
                return false;
        } else if (!timeSecs.equals(other.timeSecs))
            return false;
        if (uptimeSecs == null) {
            if (other.uptimeSecs != null)
                return false;
        } else if (!uptimeSecs.equals(other.uptimeSecs))
            return false;
        return true;
    }

}
