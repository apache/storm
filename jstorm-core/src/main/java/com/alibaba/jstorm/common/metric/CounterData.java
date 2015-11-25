package com.alibaba.jstorm.common.metric;


import com.alibaba.jstorm.metric.Bytes;
import com.alibaba.jstorm.metric.KVSerializable;

/**
 * @author wange
 * @since 15/6/23
 */
public class CounterData extends MetricBaseData implements KVSerializable {
    private long v;

    public long getV() {
        return v;
    }

    public void setV(long v) {
        this.v = v;
    }

    @Override
    public byte[] getValue() {
        return Bytes.toBytes(v);
    }

    @Override
    public Object fromKV(byte[] key, byte[] value) {
        parseKey(key);
        this.v = Bytes.toLong(value);

        return this;
    }
}
