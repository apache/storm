package com.alibaba.jstorm.common.metric.snapshot;

/**
 * @author wange
 * @since 15/6/5
 */
public class AsmCounterSnapshot extends AsmSnapshot {
    private static final long serialVersionUID = -7574994037947802582L;

    private long v;

    public long getV() {
        return v;
    }

    public AsmSnapshot setValue(long value) {
        this.v = value;
        return this;
    }
}
