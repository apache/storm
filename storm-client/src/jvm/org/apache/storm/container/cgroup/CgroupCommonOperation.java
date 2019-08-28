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

import java.io.IOException;
import java.util.Set;

public interface CgroupCommonOperation {

    /**
     * add task to cgroup.
     *
     * @param taskid task id of task to add
     */
    void addTask(int taskid) throws IOException;

    /**
     * Get a list of task ids running in CGroup.
     */
    Set<Integer> getTasks() throws IOException;

    /**
     * add a process to cgroup.
     *
     * @param pid the PID of the process to add
     */
    void addProcs(int pid) throws IOException;

    /**
     * get the PIDs of processes running in cgroup.
     */
    Set<Long> getPids() throws IOException;

    /**
     * to get the notify_on_release config.
     */
    boolean getNotifyOnRelease() throws IOException;

    /**
     * to set notify_on_release config in cgroup.
     */
    void setNotifyOnRelease(boolean flag) throws IOException;

    /**
     * get the command for the relase agent to execute.
     */
    String getReleaseAgent() throws IOException;

    /**
     * set a command for the release agent to execute.
     */
    void setReleaseAgent(String command) throws IOException;

    /**
     * get the cgroup.clone_children config.
     */
    boolean getCgroupCloneChildren() throws IOException;

    /**
     * Set the cgroup.clone_children config.
     */
    void setCgroupCloneChildren(boolean flag) throws IOException;

    /**
     * set event control config.
     */
    void setEventControl(String eventFd, String controlFd, String... args) throws IOException;
}
