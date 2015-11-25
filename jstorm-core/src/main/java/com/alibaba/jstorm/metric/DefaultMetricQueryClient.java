package com.alibaba.jstorm.metric;

import com.alibaba.jstorm.common.metric.MetricMeta;
import com.alibaba.jstorm.common.metric.TaskTrack;
import com.alibaba.jstorm.common.metric.TopologyHistory;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * a dummy metric query client implementation
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class DefaultMetricQueryClient implements MetricQueryClient {
    @Override
    public void init(Map conf) {
    }

    @Override
    public List<MetricMeta> getMetricMeta(String clusterName, String topologyId, MetaType type, MetaFilter filter, Object arg) {
        return Lists.newArrayList();
    }

    @Override
    public List<MetricMeta> getMetricMeta(String clusterName, String topologyId, MetaType type) {
        return Lists.newArrayList();
    }

    @Override
    public List<MetricMeta> getWorkerMeta(String clusterName, String topologyId) {
        return Lists.newArrayList();
    }

    @Override
    public List<MetricMeta> getNettyMeta(String clusterName, String topologyId) {
        return Lists.newArrayList();
    }

    @Override
    public List<MetricMeta> getTaskMeta(String clusterName, String topologyId, int taskId) {
        return Lists.newArrayList();
    }

    @Override
    public List<MetricMeta> getComponentMeta(String clusterName, String topologyId, String componentId) {
        return Lists.newArrayList();
    }

    @Override
    public MetricMeta getMetricMeta(String clusterName, String topologyId, MetaType metaType, long metricId) {
        return null;
    }

    @Override
    public List<Object> getMetricData(long metricId, MetricType metricType, int win, long start, long end) {
        return Lists.newArrayList();
    }

    @Override
    public List<TaskTrack> getTaskTrack(String clusterName, String topologyId) {
        return Lists.newArrayList();
    }

    @Override
    public List<TaskTrack> getTaskTrack(String clusterName, String topologyId, int taskId) {
        return Lists.newArrayList();
    }

    @Override
    public List<TopologyHistory> getTopologyHistory(String clusterName, String topologyName, int size) {
        return Lists.newArrayList();
    }

    @Override
    public void deleteMeta(MetricMeta meta) {
    }

    @Override
    public void deleteMeta(List<MetricMeta> metaList) {
    }
}
