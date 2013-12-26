package com.alibaba.jstorm.schedule.default_assign;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.resource.ResourceAssignment;

class UserDefinePreAssign implements IPreassignTask{
    private static final Logger LOG                                     = Logger
            .getLogger(UserDefinePreAssign.class);
    
    

    
    

    @Override
    public ResourceAssignment preAssign(Integer task,
                                        DefaultTopologyAssignContext defaultContext,
                                        Map componentMap, String componentName,
                                        Set<String> canUsedSupervisorIds,
                                        Map<Integer, ResourceAssignment> alreadyAssign,
                                        Map<Integer, ResourceAssignment> newAssigns) {

        ResourceAssignment taskAlloc = AssignTaskUtils.getUserDefineAlloc(task, defaultContext, componentMap,
            componentName, true);
        if (taskAlloc == null) {
            return null;
        }

        boolean resourceValid = AssignTaskUtils.checkResourceValid(taskAlloc, task, defaultContext, componentMap,
            componentName, canUsedSupervisorIds, alreadyAssign);
        if (resourceValid == false) {
            return null;
        }
        LOG.info(componentName + " of " + task + " use user-define assignment " + taskAlloc);

        AssignTaskUtils.allocResource(defaultContext, taskAlloc);
        return taskAlloc;

    }
    
}
