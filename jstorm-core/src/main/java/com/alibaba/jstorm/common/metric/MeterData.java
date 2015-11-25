package com.alibaba.jstorm.common.metric;


import com.alibaba.jstorm.metric.Bytes;
import com.alibaba.jstorm.metric.KVSerializable;

/**
 * @author wange
 * @since 15/6/23
 */
public class MeterData extends MetricBaseData implements KVSerializable {
    private double m1;
    private double m5;
    private double m15;
    private double mean;

    public double getM1() {
        return m1;
    }

    public void setM1(double m1) {
        this.m1 = m1;
    }

    public double getM5() {
        return m5;
    }

    public void setM5(double m5) {
        this.m5 = m5;
    }

    public double getM15() {
        return m15;
    }

    public void setM15(double m15) {
        this.m15 = m15;
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    @Override
    public byte[] getValue() {
        byte[] ret = new byte[8 * 4];
        Bytes.putDouble(ret, 0, m1);
        Bytes.putDouble(ret, 8, m5);
        Bytes.putDouble(ret, 16, m15);
        Bytes.putDouble(ret, 24, mean);

        return ret;
    }

    @Override
    public Object fromKV(byte[] key, byte[] value) {
        parseKey(key);

        this.m1 = Bytes.toDouble(value, 0);
        this.m5 = Bytes.toDouble(value, 8);
        this.m15 = Bytes.toDouble(value, 16);
        this.mean = Bytes.toDouble(value, 24);

        return this;
    }
}
