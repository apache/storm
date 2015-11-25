package com.alibaba.jstorm.common.metric;

import com.alibaba.jstorm.metric.Bytes;
import com.alibaba.jstorm.metric.KVSerializable;

import java.util.Date;

/**
 * @author wange
 * @since 15/7/22
 */
public abstract class MetricBaseData implements KVSerializable {
    protected long metricId;
    protected int win;
    protected Date ts;

    public long getMetricId() {
        return metricId;
    }

    public void setMetricId(long metricId) {
        this.metricId = metricId;
    }

    public Date getTs() {
        return ts;
    }

    public void setTs(Date ts) {
        this.ts = ts;
    }

    public int getWin() {
        return win;
    }

    public void setWin(int win) {
        this.win = win;
    }

    @Override
    public byte[] getKey() {
        return makeKey(metricId, win, ts.getTime());
    }

    public static byte[] makeKey(long metricId, int win, long ts) {
        byte[] ret = new byte[8 + 4 + 8];
        Bytes.putLong(ret, 0, metricId);
        Bytes.putInt(ret, 8, win);
        Bytes.putLong(ret, 12, ts);
        return ret;
    }

    protected void parseKey(byte[] key) {
        this.metricId = Bytes.toLong(key, 0, KVSerializable.LONG_SIZE);
        this.win = Bytes.toInt(key, 8, KVSerializable.INT_SIZE);
        this.ts = new Date(Bytes.toLong(key, 12, KVSerializable.LONG_SIZE));
    }
}
