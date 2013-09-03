package com.alipay.dw.jstorm.task.comm;

import java.util.List;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.alipay.dw.jstorm.daemon.worker.WorkerTransfer;

/**
 * Send init/ack/fail tuple to acker
 * 
 * @author yannian
 * 
 */

public class UnanchoredSend {
    public static void send(TopologyContext topologyContext,
            TaskSendTargets taskTargets, WorkerTransfer transfer_fn,
            String stream, List<Object> values) {
        
        java.util.List<Integer> tasks = taskTargets.get(stream, values);
        if (tasks.size() == 0) {
            return;
        }
        
        Integer taskId = topologyContext.getThisTaskId();
        
        Tuple tup = new Tuple(topologyContext, values, taskId, stream);
        
        for (Integer task : tasks) {
            transfer_fn.transfer(task, tup);
        }
    }
}
