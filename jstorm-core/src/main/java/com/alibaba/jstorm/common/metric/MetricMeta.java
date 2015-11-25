package com.alibaba.jstorm.common.metric;

import com.alibaba.jstorm.metric.KVSerializable;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.Date;

/**
 * @author wange
 * @since 15/6/18
 */
public class MetricMeta implements KVSerializable {
    // common
    private long id;
    // string id
    private String sid;
    private String clusterName;
    private String topologyId;
    private int metricType;
    private String metricGroup = MetricUtils.DEFAULT_GROUP;//sys group
    private String metricName;
    private Date gmtCreate = new Date();

    // task meta
    private String component = MetricUtils.EMPTY;
    private int taskId = 0;
    private String streamId = MetricUtils.EMPTY;
    private int metaType;

    // worker meta
    private String host = MetricUtils.EMPTY;
    private int port = 0;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
        this.sid = id + "";
    }

    public String getSid() {
        return sid;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getMetricType() {
        return metricType;
    }

    public void setMetricType(int metricType) {
        this.metricType = metricType;
    }

    public String getMetricGroup() {
        return metricGroup;
    }

    public void setMetricGroup(String metricGroup) {
        this.metricGroup = metricGroup;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public String getComponent() {
        return component;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public int getMetaType() {
        return metaType;
    }

    public void setMetaType(int metaType) {
        this.metaType = metaType;
    }

    public boolean isWorkerMetric() {
        return this.metaType == MetaType.NETTY.getT() || this.getMetaType() == MetaType.WORKER.getT() ||
                this.metaType == MetaType.TOPOLOGY.getT();
    }

    public String getFQN() {
        MetaType meta = MetaType.parse(metaType);
        MetricType metric = MetricType.parse(metricType);
        String types = meta.getV() + metric.getV();
        if (isWorkerMetric()) {
            return MetricUtils.concat2(types, topologyId, host, port, metricGroup, metricName);
        }
        return MetricUtils.concat2(types, topologyId, component, taskId, streamId, metricGroup, metricName);
    }

    /**
     * key: clusterName + topologyId + metaType + id
     */
    @Override
    public byte[] getKey() {
        StringBuilder sb = new StringBuilder(64);
        sb.append(clusterName).append(MetricUtils.AT).append(topologyId).append(MetricUtils.AT)
                .append(metaType).append(MetricUtils.AT).append(id);
        return sb.toString().getBytes();
    }

    /**
     * value: component + taskId + streamId + metricType + host + port + metricGroup + metricName
     */
    @Override
    public byte[] getValue() {
        StringBuilder sb = new StringBuilder(64);
        sb.append(component).append(MetricUtils.AT).append(taskId).append(MetricUtils.AT)
                .append(streamId).append(MetricUtils.AT).append(metricType).append(MetricUtils.AT)
                .append(host).append(MetricUtils.AT).append(port).append(MetricUtils.AT)
                .append(metricGroup).append(MetricUtils.AT).append(metricName);
        return sb.toString().getBytes();
    }

    @Override
    public Object fromKV(byte[] key, byte[] value) {
        String[] keyParts = new String(key).split(MetricUtils.DELIM);
        if (keyParts.length >= 4) {
            this.clusterName = keyParts[0];
            this.topologyId = keyParts[1];
            this.metaType = Integer.valueOf(keyParts[2]);
            this.id = Long.valueOf(keyParts[3]);
            this.sid = this.id + "";
        }
        String[] valueParts = new String(value).split(MetricUtils.DELIM);
        if (valueParts.length >= 8) {
            this.component = valueParts[0];
            this.taskId = JStormUtils.parseInt(valueParts[1], 0);
            this.streamId = valueParts[2];
            this.metricType = JStormUtils.parseInt(valueParts[3], 0);
            this.host = valueParts[4];
            this.port = JStormUtils.parseInt(valueParts[5], 0);
            this.metricGroup = valueParts[6];
            this.metricName = valueParts[7];
        }
        return this;
    }

    public static MetricMeta parse(String name) {
        return MetricMetaParser.fromMetricName(name);
    }

}
