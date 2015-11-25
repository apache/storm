package com.alibaba.jstorm.common.metric.snapshot;

import com.codahale.metrics.Snapshot;

/**
 * @author wange
 * @since 15/6/5
 */
public class AsmTimerSnapshot extends AsmSnapshot {
    private static final long serialVersionUID = 7784062881728741781L;

    private Snapshot histogram;
    private AsmMeterSnapshot meter;

    public Snapshot getHistogram() {
        return histogram;
    }

    public AsmTimerSnapshot setHistogram(Snapshot snapshot) {
        this.histogram = snapshot;
        return this;
    }

    public AsmMeterSnapshot getMeter() {
        return meter;
    }

    public AsmTimerSnapshot setMeter(AsmMeterSnapshot meter) {
        this.meter = meter;
        return this;
    }
}
