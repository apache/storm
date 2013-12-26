package com.alibaba.jstorm.utils;

import java.util.List;
import java.util.Map;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.resource.ResourceAssignment;

public class JStormServerConfig extends ConfigExtension {
    /**
     * MEM_SLOT_PER_SIZE can only be set by server
     * 
     * @param conf
     * @param slotSize
     */
    public static void setMemSlotSize(Map conf, long slotSize) {
        conf.put(MEM_SLOT_PER_SIZE, Long.valueOf(slotSize));
    }

    /**
     * TASK_ASSIGN_DISK_SLOT can only be set by server
     * 
     * @param conf
     * @param diskSlot
     */
    public static void setTaskAssignDiskSlot(Map conf, String diskSlot) {
        conf.put(TASK_ASSIGN_DISK_SLOT, diskSlot);
    }
    

    /**
     * How many cpu slot one supervisor can support , 
     * if it is null, it will be set by JStorm
     */
    protected static final String SUPERVISOR_CPU_SLOT_NUM = "supervisor.cpu.slot.num";

    public static void setSupervisorCpuSlotNum(Map conf, int slotNum) {
        conf.put(SUPERVISOR_CPU_SLOT_NUM, Integer.valueOf(slotNum));
    }

    public static Integer getSupervisorCpuSlotNum(Map conf) {
        return (Integer) conf.get(SUPERVISOR_CPU_SLOT_NUM);
    }

    /**
     * How many memory slot one supervisor can support , 
     * if it is null, it will be set by JStorm
     */
    protected static final String SUPERVISOR_MEM_SLOT_NUM = "supervisor.mem.slot.num";

    public static void setSupervisorMemSlotNum(Map conf, int slotNum) {
        conf.put(SUPERVISOR_MEM_SLOT_NUM, Integer.valueOf(slotNum));
    }

    public static Integer getSupervisorMemSlotNum(Map conf) {
        return (Integer) conf.get(SUPERVISOR_MEM_SLOT_NUM);
    }

    /**
     *  # How much disk slot one supervisor can support
        # if it is null, it will use $(storm.local.dir)/worker_shared_data
        supervisor.disk.slot: null
        # if use multiple disks, it can be set as the following
        #supervisor.disk.slot:
        #   - /disk0/jstorm/data
        #   - /disk1/jstorm/data
        #   - /disk2/jstorm/data
        #   - /disk3/jstorm/data
     */
    protected static final String SUPERVISOR_DISK_SLOTS = "supervisor.disk.slot";

    public static void setSupervisorDiskSlots(Map conf, List<String> slots) {
        conf.put(SUPERVISOR_DISK_SLOTS, slots);
    }

    public static List<String> getSupervisorDiskSlots(Map conf) {
        return (List<String>) conf.get(SUPERVISOR_DISK_SLOTS);
    }

    /**
     * The following setting will influence which task will be assigned firstly, 
     * which one will be assigned later
     * 
     * by default 
     * TOPOLOGY_TASK_ON_DIFFERENT_NODE_WEIGHT is 2
     * TOPOLOGY_USE_OLD_ASSIGN_RATIO_WEIGHT is 2
     * TOPOLOGY_USER_DEFINE_ASSIGN_RATIO_WEIGHT is 2
     */
    protected static final String TOPOLOGY_TASK_ON_DIFFERENT_NODE_WEIGHT   = "topology.task.on.different.node.weight";
    protected static final String TOPOLOGY_USE_OLD_ASSIGN_RATIO_WEIGHT     = "topology.use.old.assign.ratio.weight";
    protected static final String TOPOLOGY_USER_DEFINE_ASSIGN_RATIO_WEIGHT = "topology.user.define.assign.ratio.weight";

    public static final int       DEFAULT_TASK_ON_DIFFERENT_NODE_WEIGHT    = 2;
    public static final int       DEFAULT_USE_OLD_ASSIGN_RATIO_WEIGHT      = 2;
    public static final int       DEFAULT_USER_DEFINE_ASSIGN_RATIO_WEIGHT  = 2;

    public static void setTopologyTaskOnDiffWeight(Map conf, int weight) {
        conf.put(TOPOLOGY_TASK_ON_DIFFERENT_NODE_WEIGHT, Integer.valueOf(weight));
    }

    public static int getTopologyTaskOnDiffWeight(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_TASK_ON_DIFFERENT_NODE_WEIGHT),
            DEFAULT_TASK_ON_DIFFERENT_NODE_WEIGHT);
    }

    public static void setTopologyUseOldAssignWeight(Map conf, int weight) {
        conf.put(TOPOLOGY_USE_OLD_ASSIGN_RATIO_WEIGHT, Integer.valueOf(weight));
    }

    public static int getTopologyUseOldAssignWeight(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_USE_OLD_ASSIGN_RATIO_WEIGHT),
            DEFAULT_USE_OLD_ASSIGN_RATIO_WEIGHT);
    }

    public static void setTopologyUserDefineAssignWeight(Map conf, int weight) {
        conf.put(TOPOLOGY_USER_DEFINE_ASSIGN_RATIO_WEIGHT, Integer.valueOf(weight));
    }

    public static int getTopologyUserDefineAssignWeight(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_USER_DEFINE_ASSIGN_RATIO_WEIGHT),
            DEFAULT_USER_DEFINE_ASSIGN_RATIO_WEIGHT);
    }
}
