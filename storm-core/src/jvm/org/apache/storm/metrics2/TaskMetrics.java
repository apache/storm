package org.apache.storm.metrics2;

import com.codahale.metrics.Counter;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TaskMetrics {
    ConcurrentMap<String, Counter> ackedByStream = new ConcurrentHashMap<>();
    ConcurrentMap<String, Counter> failedByStream = new ConcurrentHashMap<>();
    ConcurrentMap<String, Counter> emittedByStream = new ConcurrentHashMap<>();
    ConcurrentMap<String, Counter> transferredByStream = new ConcurrentHashMap<>();

    private String topologyId;
    private String componentId;
    private Integer taskId;
    private Integer workerPort;

    public TaskMetrics(WorkerTopologyContext context, String componentId, Integer taskid){
        this.topologyId = context.getStormId();
        this.componentId = componentId;
        this.taskId = taskid;
        this.workerPort = context.getThisWorkerPort();
    }

    public Counter getAcked(String streamId) {
        Counter c = this.ackedByStream.get(streamId);
        if (c == null) {
            c = StormMetricRegistry.counter("acked", this.topologyId, this.componentId, this.taskId, this.workerPort, streamId);
            this.ackedByStream.put(streamId, c);
        }
        return c;
    }

    public Counter getFailed(String streamId) {
        Counter c = this.ackedByStream.get(streamId);
        if (c == null) {
            c = StormMetricRegistry.counter("failed", this.topologyId, this.componentId, this.taskId, this.workerPort, streamId);
            this.failedByStream.put(streamId, c);
        }
        return c;
    }

    public Counter getEmitted(String streamId) {
        Counter c = this.emittedByStream.get(streamId);
        if (c == null) {
            c = StormMetricRegistry.counter("emitted", this.topologyId, this.componentId, this.taskId, this.workerPort, streamId);
            this.emittedByStream.put(streamId, c);
        }
        return c;
    }

    public Counter getTransferred(String streamId) {
        Counter c = this.transferredByStream.get(streamId);
        if (c == null) {
            c = StormMetricRegistry.counter("transferred", this.topologyId, this.componentId, this.taskId, this.workerPort, streamId);
            this.transferredByStream.put(streamId, c);
        }
        return c;
    }

    public static Map<Integer, TaskMetrics> taskMetricsMap(Integer startTaskId, Integer endTaskId, WorkerTopologyContext context, String componentId){
        Map<Integer, TaskMetrics> retval = new HashMap<>();
        for (int i = startTaskId; i < endTaskId + 1; i++) {
            retval.put(i, new TaskMetrics(context, componentId, i));
        }
        return retval;
    }
}
