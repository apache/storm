package org.apache.storm.utils;

import static org.apache.storm.metrics2.TaskMetrics.METRIC_NAME_EXECUTE_JITTER;
import static org.apache.storm.metrics2.TaskMetrics.METRIC_NAME_PROCESS_JITTER;

import java.util.Comparator;
import java.util.Map;
import org.apache.storm.executor.Executor;
import org.apache.storm.tuple.AddressedTuple;

public class TaskJitterComparator implements Comparator<AddressedTuple> {
    private final Executor executor;

    public TaskJitterComparator(Executor executor) {
        this.executor = executor;
    }
    @Override
    public int compare(AddressedTuple t1, AddressedTuple t2) {
        int taskId1 = t1.getDest();
        int taskId2 = t2.getDest();

        if (taskId1 == taskId2) {
            return 0;
        }

        Map<String, Double> task1Stats = executor.getChildEwmaAvgStats(taskId1);
        Map<String, Double> task2Stats = executor.getChildEwmaAvgStats(taskId2);

        double processJitter1 = task1Stats.getOrDefault(METRIC_NAME_PROCESS_JITTER, Double.MAX_VALUE);
        double processJitter2 = task2Stats.getOrDefault(METRIC_NAME_PROCESS_JITTER, Double.MAX_VALUE);

        // compare process jitter
        if (processJitter1 < processJitter2) {
            return -1;
        } else if (processJitter1 > processJitter2) {
            return 1;
        }

        // fallback on execution jitter (it means that the network jitter is not affecting too much the whole process)
        double execJitter1 = task1Stats.getOrDefault(METRIC_NAME_EXECUTE_JITTER, Double.MAX_VALUE);
        double execJitter2 = task2Stats.getOrDefault(METRIC_NAME_EXECUTE_JITTER, Double.MAX_VALUE);
        return Double.compare(execJitter1, execJitter2);
    }
}