package com.alibaba.jstorm.common.metric;


import com.alibaba.jstorm.metric.Bytes;
import com.alibaba.jstorm.metric.KVSerializable;

/**
 * @author wange
 * @since 15/6/23
 */
public class TimerData extends MetricBaseData implements KVSerializable {
    private long min;
    private long max;
    private double mean;
    private double p50;
    private double p75;
    private double p95;
    private double p98;
    private double p99;
    private double p999;
    private double stddev;

    private double m1;
    private double m5;
    private double m15;

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public double getP50() {
        return p50;
    }

    public void setP50(double p50) {
        this.p50 = p50;
    }

    public double getP75() {
        return p75;
    }

    public void setP75(double p75) {
        this.p75 = p75;
    }

    public double getP95() {
        return p95;
    }

    public void setP95(double p95) {
        this.p95 = p95;
    }

    public double getP98() {
        return p98;
    }

    public void setP98(double p98) {
        this.p98 = p98;
    }

    public double getP99() {
        return p99;
    }

    public void setP99(double p99) {
        this.p99 = p99;
    }

    public double getP999() {
        return p999;
    }

    public void setP999(double p999) {
        this.p999 = p999;
    }

    public double getStddev() {
        return stddev;
    }

    public void setStddev(double stddev) {
        this.stddev = stddev;
    }

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

    @Override
    public byte[] getValue() {
        byte[] ret = new byte[8 * 12];
        Bytes.putLong(ret, 0, min);
        Bytes.putLong(ret, 8, max);
        Bytes.putDouble(ret, 16, p50);
        Bytes.putDouble(ret, 24, p75);
        Bytes.putDouble(ret, 32, p95);
        Bytes.putDouble(ret, 40, p98);
        Bytes.putDouble(ret, 48, p99);
        Bytes.putDouble(ret, 56, p999);
        Bytes.putDouble(ret, 64, mean);
        Bytes.putDouble(ret, 72, m1);
        Bytes.putDouble(ret, 80, m5);
        Bytes.putDouble(ret, 88, m15);

        return ret;
    }

    @Override
    public Object fromKV(byte[] key, byte[] value) {
        parseKey(key);

        this.min = Bytes.toLong(value, 0, KVSerializable.LONG_SIZE);
        this.max = Bytes.toLong(value, 8, KVSerializable.LONG_SIZE);
        this.p50 = Bytes.toDouble(value, 16);
        this.p75 = Bytes.toDouble(value, 24);
        this.p95 = Bytes.toDouble(value, 32);
        this.p98 = Bytes.toDouble(value, 40);
        this.p99 = Bytes.toDouble(value, 48);
        this.p999 = Bytes.toDouble(value, 56);
        this.mean = Bytes.toDouble(value, 64);
        this.m1 = Bytes.toDouble(value, 72);
        this.m5 = Bytes.toDouble(value, 80);
        this.m15 = Bytes.toDouble(value, 88);

        return this;
    }
}
