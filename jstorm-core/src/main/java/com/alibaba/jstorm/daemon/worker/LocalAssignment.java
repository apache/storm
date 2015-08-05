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
package com.alibaba.jstorm.daemon.worker;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Supervisor LocalAssignment
 * 
 */
public class LocalAssignment implements Serializable {
    public static final long serialVersionUID = 4054639727225043554L;
    private final String topologyId;
    private final String topologyName;
    private Set<Integer> taskIds;
    private long mem;
    private int cpu;
    private String jvm;
    private long timeStamp;

    public LocalAssignment(String topologyId, Set<Integer> taskIds,
            String topologyName, long mem, int cpu, String jvm, long timeStamp) {
        this.topologyId = topologyId;
        this.taskIds = new HashSet<Integer>(taskIds);
        this.topologyName = topologyName;
        this.mem = mem;
        this.cpu = cpu;
        this.jvm = jvm;
        this.timeStamp = timeStamp;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public Set<Integer> getTaskIds() {
        return taskIds;
    }

    public void setTaskIds(Set<Integer> taskIds) {
        this.taskIds = new HashSet<Integer>(taskIds);
    }

    public String getTopologyName() {
        return topologyName;
    }

    public String getJvm() {
        return jvm;
    }

    public void setJvm(String jvm) {
        this.jvm = jvm;
    }

    public long getMem() {
        return mem;
    }

    public void setMem(long mem) {
        this.mem = mem;
    }

    public int getCpu() {
        return cpu;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + cpu;
        result = prime * result + ((jvm == null) ? 0 : jvm.hashCode());
        result = prime * result + (int) (mem ^ (mem >>> 32));
        result = prime * result + ((taskIds == null) ? 0 : taskIds.hashCode());
        result =
                prime * result
                        + ((topologyId == null) ? 0 : topologyId.hashCode());
        result =
                prime
                        * result
                        + ((topologyName == null) ? 0 : topologyName.hashCode());
        result = prime * result + (int) (timeStamp & 0xffffffff);
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
        LocalAssignment other = (LocalAssignment) obj;
        if (cpu != other.cpu)
            return false;
        if (jvm == null) {
            if (other.jvm != null)
                return false;
        } else if (!jvm.equals(other.jvm))
            return false;
        if (mem != other.mem)
            return false;
        if (taskIds == null) {
            if (other.taskIds != null)
                return false;
        } else if (!taskIds.equals(other.taskIds))
            return false;
        if (topologyId == null) {
            if (other.topologyId != null)
                return false;
        } else if (!topologyId.equals(other.topologyId))
            return false;
        if (topologyName == null) {
            if (other.topologyName != null)
                return false;
        } else if (!topologyName.equals(other.topologyName))
            return false;
        if (timeStamp != other.getTimeStamp())
            return false;
        return true;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
