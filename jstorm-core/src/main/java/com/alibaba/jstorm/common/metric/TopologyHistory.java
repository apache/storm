package com.alibaba.jstorm.common.metric;

import com.alibaba.jstorm.metric.KVSerializable;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.Date;

/**
 * @author wange
 * @since 15/7/16
 */
public class TopologyHistory implements KVSerializable {

    private long id;
    private String clusterName;
    private String topologyName;
    private String topologyId;
    private double sampleRate;
    private Date start;
    private Date end;

    public TopologyHistory() {
    }

    public TopologyHistory(String clusterName, String topologyId) {
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

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
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

    public Date getTime() {
        return start != null ? start : end;
    }

    public String getTag() {
        return start != null ? KVSerializable.START : KVSerializable.END;
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(Double sampleRate) {
        if (sampleRate == null) {
            this.sampleRate = 1.0d;
        } else {
            this.sampleRate = sampleRate;
        }
    }

    /**
     * key: clusterName + topologyName + time
     */
    @Override
    public byte[] getKey() {
        return MetricUtils.concat2(clusterName, topologyName, getTime().getTime()).getBytes();

    }

    /**
     * value: topologyId + type: S/E
     */
    @Override
    public byte[] getValue() {
        return MetricUtils.concat2(topologyId, getTag(), sampleRate).getBytes();
    }

    @Override
    public Object fromKV(byte[] key, byte[] value) {
        String[] keyParts = new String(key).split(MetricUtils.DELIM);
        long time = 0;
        if (keyParts.length >= 3) {
            this.clusterName = keyParts[0];
            this.topologyName = keyParts[1];
            time = Long.valueOf(keyParts[2]);
        }

        String[] valueParts = new String(value).split(MetricUtils.DELIM);
        if (valueParts.length >= 3) {
            this.topologyId = valueParts[0];
            String tag = valueParts[1];
            if (tag.equals(KVSerializable.START)) {
                this.start = new Date(time);
            } else {
                this.end = new Date(time);
            }
            this.sampleRate = JStormUtils.parseDouble(valueParts[2], 0.1d);
        }

        return this;
    }

    public String getIdentity(){
        return MetricUtils.concat2(clusterName, topologyId);
    }

    public void merge(TopologyHistory history){
        if (history.start != null && this.start == null){
            this.start = history.start;
        }
        if (history.end != null && this.end == null){
            this.end = history.end;
        }
    }
}
