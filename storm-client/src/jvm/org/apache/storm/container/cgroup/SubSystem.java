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

package org.apache.storm.container.cgroup;

/**
 * a class that implements operations that can be performed on a cgroup subsystem.
 */
public class SubSystem {

    private SubSystemType type;

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    private int hierarchyID;

    private int cgroupsNum;

    private boolean enable;

    public SubSystem(SubSystemType type, int hierarchyId, int cgroupNum, boolean enable) {
        this.type = type;
        this.hierarchyID = hierarchyId;
        this.cgroupsNum = cgroupNum;
        this.enable = enable;
    }

    public SubSystemType getType() {
        return type;
    }

    public void setType(SubSystemType type) {
        this.type = type;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public int getHierarchyID() {
        return hierarchyID;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public void setHierarchyID(int hierarchyID) {
        this.hierarchyID = hierarchyID;
    }

    public int getCgroupsNum() {
        return cgroupsNum;
    }

    public void setCgroupsNum(int cgroupsNum) {
        this.cgroupsNum = cgroupsNum;
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + hierarchyID;
        return result;
    }

    @Override
    public boolean equals(Object object) {
        boolean ret = false;
        if (object != null && object instanceof SubSystem) {
            ret = ((this.type == ((SubSystem) object).getType())
                   && (this.hierarchyID == ((SubSystem) object).getHierarchyID()));
        }
        return ret;
    }
}
