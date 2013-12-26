package com.alibaba.jstorm.schedule;

import java.util.Map;

import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;

public interface IToplogyScheduler {
    void prepare(Map conf);
    
    Map<Integer, ResourceAssignment> assignTasks(TopologyAssignContext contex) 
            throws FailedAssignTopologyException;
}
