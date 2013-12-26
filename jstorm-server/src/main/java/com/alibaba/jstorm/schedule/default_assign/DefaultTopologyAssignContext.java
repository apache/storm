package com.alibaba.jstorm.schedule.default_assign;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.ThriftTopologyUtils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormServerConfig;
import com.alibaba.jstorm.utils.JStormUtils;

public class DefaultTopologyAssignContext extends TopologyAssignContext {

    private final StormTopology                    sysTopology;
    private final Map<String, String>              sidToHostname;
    private final Map<String, List<String>>        hostToSid;
    private final Map<WorkerSlot, List<Integer>>   oldWorkerTasks;
    private final Map<String, List<Integer>>       componentTasks;
    private final Map<Integer, ResourceAssignment> unstoppedAssignments = new HashMap<Integer, ResourceAssignment>();
    private final int                              totalWorkerNum;
    private final int                              unstoppedWorkerNum;

    public final int                               DISK_WEIGHT;
    public final int                               CPU_WEIGHT;
    public final int                               MEM_WEIGHT;
    public final int                               PORT_WEIGHT;
    public final int                               TASK_ON_DIFFERENT_NODE_WEIGHT;
    public final int                               USE_OLD_ASSIGN_RATIO_WEIGHT;
    public final int                               USER_DEFINE_ASSIGN_RATIO_WEIGHT;
    public final int                               DEFAULT_WEIGHT;

    private int computeWorkerNum() {
        Integer settingNum = JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_WORKERS));

        int hintSum = 0;

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
            hintSum += hint;
        }

        if (settingNum == null) {
            return hintSum;
        } else {
            return Math.min(settingNum, hintSum);
        }
    }

    public int computeUnstoppedAssignments() {

        Set<WorkerSlot> deadSupervisorWorkerSlots = new HashSet<WorkerSlot>();

        for (Integer task : unstoppedTaskIds) {
            // if unstoppedTasksIds isn't empty, it should be REASSIGN/Monitor
            ResourceAssignment resourceAssignment = oldAssignment.getTaskToResource().get(task);

            unstoppedAssignments.put(task, resourceAssignment);

            WorkerSlot workerSlot = new WorkerSlot(resourceAssignment.getSupervisorId(),
                resourceAssignment.getPort());

            deadSupervisorWorkerSlots.add(workerSlot);
        }

        return deadSupervisorWorkerSlots.size();
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

            for (List<Integer> workerTasks : oldWorkerTasks.values()) {
                if (workerTasks.contains(task)) {
                    refineDeadTasks.addAll(workerTasks);

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

        /**
         * Init priorty weight
         */
        DISK_WEIGHT = ConfigExtension.getTopologyDiskWeight(stormConf);
        CPU_WEIGHT = ConfigExtension.getTopologyCpuWeight(stormConf);
        MEM_WEIGHT = ConfigExtension.getTopologyMemWeight(stormConf);
        PORT_WEIGHT = ConfigExtension.getTopologyPortWeight(stormConf);
        DEFAULT_WEIGHT = CPU_WEIGHT + MEM_WEIGHT;

        TASK_ON_DIFFERENT_NODE_WEIGHT = JStormServerConfig.getTopologyTaskOnDiffWeight(stormConf);
        USE_OLD_ASSIGN_RATIO_WEIGHT = JStormServerConfig.getTopologyUseOldAssignWeight(stormConf);
        USER_DEFINE_ASSIGN_RATIO_WEIGHT = JStormServerConfig.getTopologyUserDefineAssignWeight(stormConf);

        

        try {
            sysTopology = Common.system_topology(stormConf, rawTopology);
        } catch (Exception e) {
            throw new FailedAssignTopologyException("Failed to generate system topology");
        }

        sidToHostname = generateSidToHost();
        hostToSid = JStormUtils.reverse_map(sidToHostname);

        if (oldAssignment != null) {
            oldWorkerTasks = Assignment.getWorkerTasks(oldAssignment.getTaskToResource());
        } else {
            oldWorkerTasks = new HashMap<WorkerSlot, List<Integer>>();
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

    public Map<WorkerSlot, List<Integer>> getOldWorkerTasks() {
        return oldWorkerTasks;
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

    public Map<Integer, ResourceAssignment> getUnstoppedAssignments() {
        return unstoppedAssignments;
    }

    @Override
    public String toString() {
        return (String) stormConf.get(Config.TOPOLOGY_NAME);
    }

    public String toDetailString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
