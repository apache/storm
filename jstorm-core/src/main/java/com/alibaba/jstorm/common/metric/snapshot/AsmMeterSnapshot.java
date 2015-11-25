package com.alibaba.jstorm.common.metric.snapshot;

/**
 * @author wange
 * @since 15/6/5
 */
public class AsmMeterSnapshot extends AsmSnapshot {
    private static final long serialVersionUID = -1754325312045025810L;

    private double m1;
    private double m5;
    private double m15;
    private double mean;

    public double getM1() {
        return m1;
    }

    public AsmMeterSnapshot setM1(double m1) {
        this.m1 = m1;
        return this;
    }

    public double getM5() {
        return m5;
    }

    public AsmMeterSnapshot setM5(double m5) {
        this.m5 = m5;
        return this;
    }

    public double getM15() {
        return m15;
    }

    public AsmMeterSnapshot setM15(double m15) {
        this.m15 = m15;
        return this;
    }

    public double getMean() {
        return mean;
    }

    public AsmMeterSnapshot setMean(double mean) {
        this.mean = mean;
        return this;
    }
}
