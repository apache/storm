package com.alibaba.jstorm.schedule.default_assign;

import java.util.Map;

import com.alibaba.jstorm.resource.ResourceAssignment;

public interface IPostAssignTask {
    void postAssign(DefaultTopologyAssignContext defaultContext,
                        Map<Integer, ResourceAssignment> newAssigns, int allocWorkerNum);
}
