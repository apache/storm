package com.alibaba.jstorm.task.group;

import java.util.List;

import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RandomRange;

public abstract class Shuffer {
    private WorkerData workerData;
    
    public Shuffer(WorkerData workerData) {
        this.workerData = workerData;
    }
    
    public abstract List<Integer> grouper(List<Object> values);
    
    protected int getActiveTask(RandomRange randomrange, List<Integer> outTasks) {
        int index = randomrange.nextInt();
        int size = outTasks.size();
        
        for(int i = 0; i < size; i++) {
            if(workerData.isOutboundTaskActive(Integer.valueOf(outTasks.get(index))))
                break;
            else
                index = randomrange.nextInt();
        }
        
        return (index < size ? index : -1);
    }
}