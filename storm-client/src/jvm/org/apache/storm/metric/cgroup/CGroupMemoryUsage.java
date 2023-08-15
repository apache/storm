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

package org.apache.storm.metric.cgroup;

import java.util.Map;
import org.apache.storm.container.cgroup.SubSystemType;
import org.apache.storm.container.cgroup.core.CgroupCore;
import org.apache.storm.container.cgroup.core.MemoryCore;

/**
 * Reports the current memory usage of the cgroup for this worker.
 */
@Deprecated
public class CGroupMemoryUsage extends CGroupMetricsBase<Long> {

    public CGroupMemoryUsage(Map<String, Object> conf) {
        super(conf, SubSystemType.memory);
    }

    @Override
    public Long getDataFrom(CgroupCore core) throws Exception {
        return ((MemoryCore) core).getPhysicalUsage();
    }
}
