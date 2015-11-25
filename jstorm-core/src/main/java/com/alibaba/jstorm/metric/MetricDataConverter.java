package com.alibaba.jstorm.metric;

import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.common.metric.*;
import com.alibaba.jstorm.utils.TimeUtils;

import java.util.Date;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class MetricDataConverter {

    public static CounterData toCounterData(MetricSnapshot snapshot, int win) {
        CounterData data = new CounterData();
        convertBase(snapshot, data, win);

        data.setV(snapshot.get_longValue());
        return data;
    }

    public static GaugeData toGaugeData(MetricSnapshot snapshot, int win) {
        GaugeData data = new GaugeData();
        convertBase(snapshot, data, win);

        data.setV(snapshot.get_doubleValue());
        return data;
    }

    public static MeterData toMeterData(MetricSnapshot snapshot, int win) {
        MeterData data = new MeterData();
        convertBase(snapshot, data, win);

        data.setM1(snapshot.get_m1());
        data.setM5(snapshot.get_m5());
        data.setM15(snapshot.get_m15());
        data.setMean(snapshot.get_mean());

        return data;
    }

    public static HistogramData toHistogramData(MetricSnapshot snapshot, int win) {
        HistogramData data = new HistogramData();
        convertBase(snapshot, data, win);

        data.setMin(snapshot.get_min());
        data.setMax(snapshot.get_max());
        data.setP50(snapshot.get_p50());
        data.setP75(snapshot.get_p75());
        data.setP95(snapshot.get_p95());
        data.setP98(snapshot.get_p98());
        data.setP99(snapshot.get_p99());
        data.setP999(snapshot.get_p999());
        data.setMean(snapshot.get_mean());

        return data;
    }

    public static TimerData toTimerData(MetricSnapshot snapshot, int win) {
        TimerData data = new TimerData();
        convertBase(snapshot, data, win);

        data.setMin(snapshot.get_min());
        data.setMax(snapshot.get_max());
        data.setP50(snapshot.get_p50());
        data.setP75(snapshot.get_p75());
        data.setP95(snapshot.get_p95());
        data.setP98(snapshot.get_p98());
        data.setP99(snapshot.get_p99());
        data.setP999(snapshot.get_p999());
        data.setMean(snapshot.get_mean());
        data.setM1(snapshot.get_m1());
        data.setM5(snapshot.get_m5());
        data.setM15(snapshot.get_m15());

        return data;
    }

    private static void convertBase(MetricSnapshot snapshot, MetricBaseData data, int win) {
        long newTs = TimeUtils.alignTimeToWin(snapshot.get_ts(), win);
        data.setWin(win);
        data.setMetricId(snapshot.get_metricId());
        data.setTs(new Date(newTs));
    }

}
