package com.alibaba.jstorm.common.metric;


import com.alibaba.jstorm.metric.Bytes;
import com.alibaba.jstorm.metric.KVSerializable;

/**
 * @author wange
 * @since 15/6/23
 */
public class GaugeData extends MetricBaseData implements KVSerializable {
    private double v;

    public double getV() {
        return v;
    }

    public void setV(double v) {
        this.v = v;
    }

    @Override
    public byte[] getValue() {
        return Bytes.toBytes(v);
    }

    @Override
    public Object fromKV(byte[] key, byte[] value) {
        parseKey(key);
        this.v = Bytes.toDouble(value);

        return this;
    }
}
