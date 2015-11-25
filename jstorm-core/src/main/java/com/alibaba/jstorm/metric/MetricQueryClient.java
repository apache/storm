package com.alibaba.jstorm.metric;

import com.alibaba.jstorm.common.metric.MetricMeta;
import com.alibaba.jstorm.common.metric.TaskTrack;
import com.alibaba.jstorm.common.metric.TopologyHistory;

import java.util.List;
import java.util.Map;

/**
 * metric query client for getting metric meta & data
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public interface MetricQueryClient {

    /**
     * init metric query client
     */
    void init(Map conf);

    /**
     * get metric meta with optional meta filter
     *
     * @param clusterName cluster name
     * @param topologyId  topology id
     * @param type        meta type
     * @param filter      meta filter, if filter matches, the corresponding meta will be returned.
     * @param arg         filter argument
     * @return meta list
     */
    List<MetricMeta> getMetricMeta(String clusterName, String topologyId, MetaType type, MetaFilter filter, Object arg);

    /**
     * get metric meta by topology id and meta type
     *
     * @param clusterName cluster name
     * @param topologyId  topology id
     * @param type        meta type
     * @return all metric meta
     */
    List<MetricMeta> getMetricMeta(String clusterName, String topologyId, MetaType type);

    /**
     * get worker metric meta by topology id
     *
     * @param clusterName cluster name
     * @param topologyId  topology id
     * @return all worker metric meta
     */
    List<MetricMeta> getWorkerMeta(String clusterName, String topologyId);

    /**
     * get netty metric meta by topology id
     *
     * @param clusterName cluster name
     * @param topologyId  topology id
     * @return all netty metric meta
     */
    List<MetricMeta> getNettyMeta(String clusterName, String topologyId);

    /**
     * get task metric meta
     *
     * @param clusterName cluster name
     * @param topologyId  topology id
     * @param taskId      task id
     * @return task metric meta
     */
    List<MetricMeta> getTaskMeta(String clusterName, String topologyId, int taskId);

    /**
     * get component metric meta
     *
     * @param clusterName cluster name
     * @param topologyId  topology id
     * @param componentId component id
     * @return component metric meta
     */
    List<MetricMeta> getComponentMeta(String clusterName, String topologyId, String componentId);

    /**
     * get metric meta by id
     *
     * @param clusterName cluster name
     * @param topologyId  topology id
     * @param metaType    meta type
     * @param metricId    metric id
     * @return metric meta
     */
    MetricMeta getMetricMeta(String clusterName, String topologyId, MetaType metaType, long metricId);

    /**
     * get metric data
     *
     * @param metricId   metric id
     * @param metricType metric type
     * @param win        metric window
     * @param start      start time
     * @param end        end time
     * @return metric data objects, depending on metric type, could be CounterData, GaugeData, ... etc.
     */
    List<Object> getMetricData(long metricId, MetricType metricType, int win, long start, long end);

    /**
     * get all task track by topology id
     *
     * @param clusterName cluster name
     * @param topologyId  topology id
     * @return task track
     */
    List<TaskTrack> getTaskTrack(String clusterName, String topologyId);

    /**
     * get task track by task id
     *
     * @param clusterName cluster name
     * @param topologyId  topology id
     * @param taskId      task id
     * @return task track
     */
    List<TaskTrack> getTaskTrack(String clusterName, String topologyId, int taskId);

    /**
     * get topology history
     *
     * @param clusterName  cluster name
     * @param topologyName topology name
     * @param size         size
     * @return topology history list
     */
    List<TopologyHistory> getTopologyHistory(String clusterName, String topologyName, int size);

    /**
     * delete metrics meta. note that clusterName, topologyId, metaType & id must be set.
     *
     * @param meta metric meta
     */
    void deleteMeta(MetricMeta meta);

    /**
     * delete metrics meta list. note that clusterName, topologyId, metaType & id must be set.
     *
     * @param metaList metric meta list
     */
    void deleteMeta(List<MetricMeta> metaList);
}
