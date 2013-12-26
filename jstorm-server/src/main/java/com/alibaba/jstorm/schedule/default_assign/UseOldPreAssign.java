package com.alibaba.jstorm.schedule.default_assign;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.resource.TaskAllocResource;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.utils.JStormUtils;

public class UseOldPreAssign implements IPreassignTask {
    private static final Logger LOG                                     = Logger
            .getLogger(UseOldPreAssign.class);
    
    private ResourceAssignment getOldAlloc(Integer task,
                                           DefaultTopologyAssignContext defaultContext,
                                           Map componentMap, String componentName) {
        Map stormConf = defaultContext.getStormConf();

        if (ConfigExtension.isUseOldAssignment(stormConf) == false
            && ConfigExtension.isUseOldAssignment(componentMap) == false) {
            return null;
        }

        Assignment oldAssign = defaultContext.getOldAssignment();
        if (oldAssign == null) {
            LOG.info("Try USE_OLD_ASSIGNMENT, but no old Assignment");
            return null;
        }

        ResourceAssignment oldAssignment = oldAssign.getTaskToResource().get(task);
        if (oldAssignment == null) {
            LOG.warn(componentName + " no old assignment of task " + task + ","
                     + defaultContext);
            return null;
        }
        
        TaskAllocResource taskAlloc = AssignTaskUtils.getTaskAlloc(task, defaultContext, componentMap, componentName);
        
        if (taskAlloc.allocEqual(oldAssignment) == false) {
            LOG.info(componentName + " of " + task + " allocation has changed, old assignment can't be used ");
            LOG.info(" new Assignment " + taskAlloc);
            LOG.info(" old Assignment  " + oldAssignment);
            return null;
        }
        
        LOG.info("Use old assignment " + oldAssignment);

        return oldAssignment;
    }

    @Override
    public ResourceAssignment preAssign(Integer task,
                                        DefaultTopologyAssignContext defaultContext,
                                        Map componentMap, String componentName,
                                        Set<String> canUsedSupervisorIds,
                                        Map<Integer, ResourceAssignment> alreadyAssign,
                                        Map<Integer, ResourceAssignment> newAssigns) {

        ResourceAssignment oldAssignment = getOldAlloc(task, defaultContext, componentMap,
            componentName);
        if (oldAssignment == null) {
            return null;
        }

        boolean resourceValid = AssignTaskUtils.checkResourceValid(oldAssignment, task, defaultContext,
            componentMap, componentName, canUsedSupervisorIds, alreadyAssign);
        if (resourceValid == false) {
            return null;
        }
        LOG.info(componentName + " of " + task + " use old assignment " + oldAssignment);

        AssignTaskUtils.allocResource(defaultContext, oldAssignment);

        return oldAssignment;

    }
    
}
