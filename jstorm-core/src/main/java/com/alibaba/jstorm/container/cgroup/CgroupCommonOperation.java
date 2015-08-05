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
package com.alibaba.jstorm.container.cgroup;

import java.io.IOException;
import java.util.Set;

public interface CgroupCommonOperation {

    public void addTask(int taskid) throws IOException;

    public Set<Integer> getTasks() throws IOException;

    public void addProcs(int pid) throws IOException;

    public Set<Integer> getPids() throws IOException;

    public void setNotifyOnRelease(boolean flag) throws IOException;

    public boolean getNotifyOnRelease() throws IOException;

    public void setReleaseAgent(String command) throws IOException;

    public String getReleaseAgent() throws IOException;

    public void setCgroupCloneChildren(boolean flag) throws IOException;

    public boolean getCgroupCloneChildren() throws IOException;

    public void setEventControl(String eventFd, String controlFd,
            String... args) throws IOException;

}
