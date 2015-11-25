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
package com.alibaba.jstorm.schedule.default_assign;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.ThriftTopologyUtils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.*;
import java.util.Map.Entry;

public class DefaultTopologyAssignContext extends TopologyAssignContext {

    private final StormTopology sysTopology;
    private final Map<String, String> sidToHostname;
    private final Map<String, List<String>> hostToSid;
    private final Set<ResourceWorkerSlot> oldWorkers;
    private final Map<String, List<Integer>> componentTasks;
    private final Set<ResourceWorkerSlot> unstoppedWorkers = new HashSet<ResourceWorkerSlot>();
    private final int totalWorkerNum;
    private final int unstoppedWorkerNum;

    private int computeWorkerNum() {
        Integer settingNum = JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_WORKERS));

        int ret = 0, hintSum = 0, tmCount = 0;

        Map<String, Object> components = ThriftTopologyUtils.getComponents(sysTopology);
        for (Entry<String, Object> entry : components.entrySet()) {
            String componentName = entry.getKey();
            Object component = entry.getValue();

            ComponentCommon common = null;
            if (component instanceof Bolt) {
                common = ((Bolt) component).get_common();
            }
            if (component instanceof SpoutSpec) {
                common = ((SpoutSpec) component).get_common();
            }
            if (component instanceof StateSpoutSpec) {
                common = ((StateSpoutSpec) component).get_common();
            }

            int hint = common.get_parallelism_hint();
            if (componentName.equals(Common.TOPOLOGY_MASTER_COMPONENT_ID)) {
                tmCount += hint;
                continue;
            }
            hintSum += hint;
        }

        if (settingNum == null) {
            ret = hintSum;
        } else {
            ret =  Math.min(settingNum, hintSum);
        }

        Boolean isTmSingleWorker = ConfigExtension.getTopologyMasterSingleWorker(stormConf);
        if (isTmSingleWorker != null) {
            if (isTmSingleWorker == true) {
                // Assign a single worker for topology master
                ret += tmCount;
                setAssignSingleWorkerForTM(true);
            }
        } else {
            // If not configured, judge this config by worker number
            if (ret >= 10) {
                ret += tmCount;
                setAssignSingleWorkerForTM(true);
            }
        }

        return ret;
    }

    public int computeUnstoppedAssignments() {
        for (Integer task : unstoppedTaskIds) {
            // if unstoppedTasksIds isn't empty, it should be REASSIGN/Monitor
            ResourceWorkerSlot worker = oldAssignment.getWorkerByTaskId(task);
            unstoppedWorkers.add(worker);
        }

        return unstoppedWorkers.size();
    }

    private void refineDeadTasks() {
        Set<Integer> rawDeadTasks = getDeadTaskIds();
        Set<Integer> refineDeadTasks = new HashSet<Integer>();
        refineDeadTasks.addAll(rawDeadTasks);

        Set<Integer> unstoppedTasks = getUnstoppedTaskIds();

        // if type is ASSIGN_NEW, rawDeadTasks is empty
        // then the oldWorkerTasks should be existingAssignment
        for (Integer task : rawDeadTasks) {
            if (unstoppedTasks.contains(task)) {
                continue;
            }
            for (ResourceWorkerSlot worker : oldWorkers) {
                if (worker.getTasks().contains(task)) {
                    refineDeadTasks.addAll(worker.getTasks());

                }
            }
        }
        setDeadTaskIds(refineDeadTasks);
    }

    /**
     * @@@ Do we need just handle the case whose type is ASSIGN_TYPE_NEW?
     * 
     * @return
     */
    private Map<String, String> generateSidToHost() {
        Map<String, String> sidToHostname = new HashMap<String, String>();
        if (oldAssignment != null) {
            sidToHostname.putAll(oldAssignment.getNodeHost());
        }

        for (Entry<String, SupervisorInfo> entry : cluster.entrySet()) {
            String supervisorId = entry.getKey();
            SupervisorInfo supervisorInfo = entry.getValue();

            sidToHostname.put(supervisorId, supervisorInfo.getHostName());

        }

        return sidToHostname;
    }

    public DefaultTopologyAssignContext(TopologyAssignContext context) {
        super(context);

        try {
            sysTopology = Common.system_topology(stormConf, rawTopology);
        } catch (Exception e) {
            throw new FailedAssignTopologyException("Failed to generate system topology");
        }

        sidToHostname = generateSidToHost();
        hostToSid = JStormUtils.reverse_map(sidToHostname);

        if (oldAssignment != null && oldAssignment.getWorkers() != null) {
            oldWorkers = oldAssignment.getWorkers();
        } else {
            oldWorkers = new HashSet<ResourceWorkerSlot>();
        }

        refineDeadTasks();

        componentTasks = JStormUtils.reverse_map(context.getTaskToComponent());

        for (Entry<String, List<Integer>> entry : componentTasks.entrySet()) {
            List<Integer> componentTaskList = entry.getValue();

            Collections.sort(componentTaskList);
        }

        totalWorkerNum = computeWorkerNum();

        unstoppedWorkerNum = computeUnstoppedAssignments();
    }

    public StormTopology getSysTopology() {
        return sysTopology;
    }

    public Map<String, String> getSidToHostname() {
        return sidToHostname;
    }

    public Map<String, List<String>> getHostToSid() {
        return hostToSid;
    }

    public Map<String, List<Integer>> getComponentTasks() {
        return componentTasks;
    }

    public int getTotalWorkerNum() {
        return totalWorkerNum;
    }

    public int getUnstoppedWorkerNum() {
        return unstoppedWorkerNum;
    }

    public Set<ResourceWorkerSlot> getOldWorkers() {
        return oldWorkers;
    }

    public Set<ResourceWorkerSlot> getUnstoppedWorkers() {
        return unstoppedWorkers;
    }

    @Override
    public String toString() {
        return (String) stormConf.get(Config.TOPOLOGY_NAME);
    }

    public String toDetailString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
