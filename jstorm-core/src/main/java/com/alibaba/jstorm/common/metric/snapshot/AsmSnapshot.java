package com.alibaba.jstorm.common.metric.snapshot;

import java.io.Serializable;

/**
 * @author wange
 * @since 15/6/5
 */
public abstract class AsmSnapshot implements Serializable {
    private static final long serialVersionUID = 1945719653840917619L;

    private long metricId;
    private long ts;

    public long getTs() {
        return ts;
    }

    public AsmSnapshot setTs(long ts) {
        this.ts = ts;
        return this;
    }

    public long getMetricId() {
        return metricId;
    }

    public AsmSnapshot setMetricId(long metricId) {
        this.metricId = metricId;
        return this;
    }
}
