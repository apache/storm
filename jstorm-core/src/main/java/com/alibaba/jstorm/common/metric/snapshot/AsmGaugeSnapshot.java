package com.alibaba.jstorm.common.metric.snapshot;

/**
 * @author wange
 * @since 15/6/5
 */
public class AsmGaugeSnapshot extends AsmSnapshot {
    private static final long serialVersionUID = 3216517772824794848L;

    private double v;

    public double getV() {
        return v;
    }

    public AsmSnapshot setValue(double value) {
        this.v = value;
        return this;
    }
}
