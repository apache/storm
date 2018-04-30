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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.storm.container.cgroup.core.BlkioCore;
import org.apache.storm.container.cgroup.core.CgroupCore;
import org.apache.storm.container.cgroup.core.CpuCore;
import org.apache.storm.container.cgroup.core.CpuacctCore;
import org.apache.storm.container.cgroup.core.CpusetCore;
import org.apache.storm.container.cgroup.core.DevicesCore;
import org.apache.storm.container.cgroup.core.FreezerCore;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.container.cgroup.core.NetClsCore;
import org.apache.storm.container.cgroup.core.NetPrioCore;

public class CgroupCoreFactory {

    public static CgroupCore getInstance(SubSystemType type, String dir) {
        switch (type) {
            case blkio:
                return new BlkioCore(dir);
            case cpuacct:
                return new CpuacctCore(dir);
            case cpuset:
                return new CpusetCore(dir);
            case cpu:
                return new CpuCore(dir);
            case devices:
                return new DevicesCore(dir);
            case freezer:
                return new FreezerCore(dir);
            case memory:
                return new MemoryCore(dir);
            case net_cls:
                return new NetClsCore(dir);
            case net_prio:
                return new NetPrioCore(dir);
            default:
                return null;
        }
    }

    public static Map<SubSystemType, CgroupCore> getInstance(Set<SubSystemType> types, String dir) {
        Map<SubSystemType, CgroupCore> result = new HashMap<SubSystemType, CgroupCore>();
        for (SubSystemType type : types) {
            CgroupCore inst = getInstance(type, dir);
            if (inst != null) {
                result.put(type, inst);
            }
        }
        return result;
    }
}
