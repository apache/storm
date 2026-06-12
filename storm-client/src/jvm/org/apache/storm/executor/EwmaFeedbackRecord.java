/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.executor;

import com.codahale.metrics.Gauge;
import java.util.Map;
import java.util.function.ObjDoubleConsumer;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.metrics2.PerReporterGauge;
import org.apache.storm.metrics2.TaskMetrics;

/**
 * Immutable snapshot of a task's jitter metrics, used as the payload of an upstream feedback tuple.
 *
 * @param processJitter  The {@code __process-jitter} gauge value, or {@link #VOID} if absent.
 * @param completeJitter The {@code __complete-jitter} gauge value, or {@link #VOID} if absent.
 * @param executeJitter  The {@code __execute-jitter} gauge value, or {@link #VOID} if absent.
 */
public record EwmaFeedbackRecord(double processJitter, double completeJitter, double executeJitter) {

    // Sentinel for an absent metric. Jitter values are always >= 0, so a negative value can never
    // collide with a real measurement and unambiguously marks "gauge missing / not a Number".
    private static final double VOID = -1;

    private static double fromGauge(Gauge<?> gauge) {
        if (gauge != null && !(gauge instanceof PerReporterGauge)) {
            Object v = gauge.getValue();
            if (v instanceof Number) {
                return ((Number) v).doubleValue();
            }
        }
        return VOID;
    }

    private static double aggregate(Map<String, Gauge> gauges, String metricName) {
        String suffixedPrefix = metricName + "-";
        double agg = VOID;
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            String name = entry.getKey();
            if (!name.equals(metricName) && !name.startsWith(suffixedPrefix)) {
                continue;
            }
            double value = fromGauge(entry.getValue());
            if (value != VOID && (agg == VOID || value > agg)) {
                agg = value;
            }
        }
        return agg;
    }

    public static EwmaFeedbackRecord fromWorkerState(WorkerState workerData, int taskId) {
        Map<String, Gauge> allGauges = workerData.getMetricRegistry().getTaskGauges(taskId);
        return new EwmaFeedbackRecord(aggregate(allGauges, TaskMetrics.METRIC_NAME_PROCESS_JITTER),
                aggregate(allGauges, TaskMetrics.METRIC_NAME_COMPLETE_JITTER),
                aggregate(allGauges, TaskMetrics.METRIC_NAME_EXECUTE_JITTER));
    }

    /**
     * Invokes {@code consumer} once for each present jitter metric.
     */
    public void forEachMetric(ObjDoubleConsumer<String> consumer) {
        if (processJitter != VOID) {
            consumer.accept(TaskMetrics.METRIC_NAME_PROCESS_JITTER, processJitter);
        }
        if (completeJitter != VOID) {
            consumer.accept(TaskMetrics.METRIC_NAME_COMPLETE_JITTER, completeJitter);
        }
        if (executeJitter != VOID) {
            consumer.accept(TaskMetrics.METRIC_NAME_EXECUTE_JITTER, executeJitter);
        }
    }
}
