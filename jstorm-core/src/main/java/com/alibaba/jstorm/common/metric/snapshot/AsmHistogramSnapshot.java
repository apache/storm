package com.alibaba.jstorm.common.metric.snapshot;

import com.codahale.metrics.Snapshot;

/**
 * @author wange
 * @since 15/6/5
 */
public class AsmHistogramSnapshot extends AsmSnapshot {
    private static final long serialVersionUID = 7284437562594156565L;

    private Snapshot snapshot;

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public AsmSnapshot setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        return this;
    }
}
