package com.alibaba.jstorm.schedule.default_assign;

import java.util.Map;
import java.util.Set;

import com.alibaba.jstorm.resource.ResourceAssignment;

public interface IPreassignTask {
    ResourceAssignment preAssign(Integer task, DefaultTopologyAssignContext defaultContext,
                                 Map componentMap, String componentName,
                                 Set<String> canUsedSupervisorIds,
                                 Map<Integer, ResourceAssignment> alreadyAssign,
                                 Map<Integer, ResourceAssignment> newAssigns);
}
