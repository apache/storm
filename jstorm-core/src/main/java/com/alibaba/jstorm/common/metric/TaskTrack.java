package com.alibaba.jstorm.common.metric;

import com.alibaba.jstorm.metric.KVSerializable;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.Date;

/**
 * @author wange
 * @since 15/7/16
 */
public class TaskTrack implements KVSerializable {

    private long id;
    private String clusterName;
    private String topologyId;
    private String component;
    private int taskId;
    private String host;
    private int port;
    private Date start;
    private Date end;

    public TaskTrack() {
    }

    public TaskTrack(String clusterName, String topologyId) {
        this.clusterName = clusterName;
        this.topologyId = topologyId;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
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

    public Date getStart() {
        return start;
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public Date getEnd() {
        return end;
    }

    public void setEnd(Date end) {
        this.end = end;
    }

    /**
     * key: clusterName + topologyId + taskId + time
     */
    @Override
    public byte[] getKey() {
        StringBuilder sb = new StringBuilder(128);
        sb.append(clusterName).append(MetricUtils.AT).append(topologyId).append(MetricUtils.AT)
                .append(taskId).append(MetricUtils.AT);
        if (start != null) {
            sb.append(start.getTime());
        } else {
            sb.append(end.getTime());
        }
        return sb.toString().getBytes();
    }

    /**
     * value: type + host + port
     * type: S/E (start/end)
     */
    @Override
    public byte[] getValue() {
        StringBuilder sb = new StringBuilder(32);
        if (start != null) {
            sb.append(KVSerializable.START);
        } else {
            sb.append(KVSerializable.END);
        }
        sb.append(MetricUtils.AT).append(host).append(MetricUtils.AT).append(port);
        return sb.toString().getBytes();
    }

    @Override
    public Object fromKV(byte[] key, byte[] value) {
        String[] keyParts = new String(key).split(MetricUtils.DELIM);

        String[] valueParts = new String(value).split(MetricUtils.DELIM);
        boolean isStart = false;
        if (valueParts.length >= 3){
            if (valueParts[0].equals(KVSerializable.START)) isStart = true;
            host = valueParts[1];
            port = JStormUtils.parseInt(valueParts[2]);
        }

        if (keyParts.length >= 4){
            clusterName = keyParts[0];
            topologyId = keyParts[1];
            taskId = JStormUtils.parseInt(keyParts[2]);
            long ts = JStormUtils.parseLong(keyParts[3]);
            if (isStart) start = new Date(ts);
            else end = new Date(ts);
        }

        return this;
    }

    public Date getTime() {
        return start != null ? start : end;
    }

    public String getIdentity(){
        StringBuilder sb = new StringBuilder();
        sb.append(clusterName).append(MetricUtils.AT).append(topologyId).append(MetricUtils.AT)
                .append(taskId).append(MetricUtils.AT).append(host).append(MetricUtils.AT).append(port);
        return sb.toString();
    }

    public void merge(TaskTrack taskTrack){
        if (taskTrack.start != null && this.start == null){
            this.start = taskTrack.start;
        }
        if (taskTrack.end != null && this.end == null){
            this.end = taskTrack.end;
        }
    }
}
