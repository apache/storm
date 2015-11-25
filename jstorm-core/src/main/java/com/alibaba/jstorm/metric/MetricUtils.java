package com.alibaba.jstorm.metric;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.TopologyMetric;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.common.metric.snapshot.*;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class MetricUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);

    public static final char AT = '@';
    public static final String DELIM = AT + "";
    public static final String EMPTY = "";
    public static final String DEFAULT_GROUP = "sys";

    public static final int MAX_POINTS_PER_WORKER = 200;
    public static final int NETTY_METRIC_PAGE_SIZE = 200;

    public static boolean isValidId(long metricId) {
        return metricId != 0;
    }

    public static MetricInfo mkMetricInfo() {
        MetricInfo ret = new MetricInfo();
        ret.set_metrics(new HashMap<String, Map<Integer, MetricSnapshot>>());

        return ret;
    }

    public static TopologyMetric mkTopologyMetric() {
        TopologyMetric emptyTopologyMetric = new TopologyMetric();

        emptyTopologyMetric.set_topologyMetric(new MetricInfo());
        emptyTopologyMetric.set_componentMetric(new MetricInfo());
        emptyTopologyMetric.set_workerMetric(new MetricInfo());
        emptyTopologyMetric.set_taskMetric(new MetricInfo());
        emptyTopologyMetric.set_streamMetric(new MetricInfo());
        emptyTopologyMetric.set_nettyMetric(new MetricInfo());

        return emptyTopologyMetric;
    }

    public static boolean isEnableNettyMetrics(Map stormConf) {
        int maxWorkerNumForNetty = ConfigExtension.getTopologyMaxWorkerNumForNettyMetrics(stormConf);
        int workerNum = JStormUtils.parseInt(stormConf.get("topology.workers"), 1);
        return workerNum < maxWorkerNumForNetty;
    }

    /**
     * a metric name composites of: type@topologyId@componentId@taskId@streamId@group@name for non-worker metrics OR type@topologyId@host@port@group@name for
     * worker metrics
     */
    public static String metricName(String type, String topologyId, String componentId, int taskId, String streamId, String group, String name) {
        return concat(type, topologyId, componentId, taskId, streamId, group, name);
    }

    public static String streamMetricName(String topologyId, String componentId, int taskId, String streamId, String name, MetricType type) {
        return concat(MetaType.STREAM.getV() + type.getV(), topologyId, componentId, taskId, streamId, DEFAULT_GROUP, name);
    }

    public static String workerMetricName(String topologyId, String host, int port, String name, MetricType type) {
        return concat(MetaType.WORKER.getV() + type.getV(), topologyId, host, port, DEFAULT_GROUP, name);
    }

    public static String workerMetricName(String name, MetricType type) {
        return concat(MetaType.WORKER.getV() + type.getV(), EMPTY, EMPTY, 0, DEFAULT_GROUP, name);
    }

    public static String nettyMetricName(String name, MetricType type) {
        return concat(MetaType.NETTY.getV() + type.getV(), EMPTY, EMPTY, 0, JStormMetrics.NETTY_GROUP, name);
    }

    public static String workerMetricPrefix(String topologyId, String host, int port) {
        return concat(MetaType.WORKER.getV(), topologyId, host, port);
    }

    public static String taskMetricName(String topologyId, String componentId, int taskId, String name, MetricType type) {
        return concat(MetaType.TASK.getV() + type.getV(), topologyId, componentId, taskId, EMPTY, DEFAULT_GROUP, name);
    }

    public static String taskMetricName(String topologyId, String componentId, int taskId, String group, String name, MetricType type) {
        return concat(MetaType.TASK.getV() + type.getV(), topologyId, componentId, taskId, EMPTY, group, name);
    }

    public static String compMetricName(String topologyId, String componentId, String name, MetricType type) {
        return concat(MetaType.COMPONENT.getV() + type.getV(), topologyId, componentId, 0, EMPTY, DEFAULT_GROUP, name);
    }

    public static String removeDelimIfPossible(String name) {
        if (name.contains(DELIM)) {
            return name.replace(DELIM, EMPTY);
        }
        return name;
    }

    public static MetaType metaType(String name) {
        return MetaType.parse(name.charAt(0) + EMPTY);
    }

    public static MetricType metricType(String name) {
        return MetricType.parse(name.charAt(1) + EMPTY);
    }

    /**
     * make streamId empty, remain other parts the same
     */
    public static String stream2taskName(String old) {
        String[] parts = old.split(DELIM);
        if (parts.length >= 7) {
            parts[0] = MetaType.TASK.getV() + parts[0].charAt(1);
            parts[parts.length - 3] = EMPTY;
        }
        return concat(parts);
    }

    /**
     * make taskId=0 and streamId empty.
     */
    public static String task2compName(String old) {
        String[] parts = old.split(DELIM);
        if (parts.length >= 7) {
            parts[0] = MetaType.COMPONENT.getV() + parts[0].charAt(1);
            parts[parts.length - 3] = EMPTY;
            parts[parts.length - 4] = "0";
        }
        return concat(parts);
    }

    /**
     * make taskId=0 and streamId empty and metricName remain the string after `.`.
     */
    public static String task2MergeCompName(String old) {
        String[] parts = old.split(DELIM);
        if (parts.length >= 7) {
            parts[0] = MetaType.COMPONENT.getV() + parts[0].charAt(1);
            parts[parts.length - 3] = EMPTY;
            parts[parts.length - 4] = "0";

            String metricName = parts[parts.length - 1];
            int dotIndex = metricName.indexOf(".");
            if (dotIndex != -1){
                metricName = metricName.substring(dotIndex+1);
                parts[parts.length - 1] = metricName;
            }
        }
        return concat(parts);
    }

    /**
     * change component metric name to worker metric name, only for topology metrics
     */
    public static String comp2topologyName(String old) {
        String[] parts = old.split(DELIM);
        parts[0] = MetaType.TOPOLOGY.getV() + parts[0].charAt(1);
        // type + topologyId + host + port + group + name
        return concat(parts[0], parts[1], EMPTY, "0", parts[5], parts[6]);
    }

    public static String worker2topologyName(String old) {
        String[] parts = old.split(DELIM);
        if (parts.length >= 5) {
            parts[0] = MetaType.TOPOLOGY.getV() + parts[0].charAt(1);
            parts[2] = EMPTY;   // host
            parts[3] = "0";     // port
        }
        return concat(parts);
    }

    public static String topo2clusterName(String old){
        String[] parts = old.split(DELIM);
        parts[1] = JStormMetrics.CLUSTER_METRIC_KEY;
        return concat(parts);
    }

    public static String concat(Object... args) {
        StringBuilder sb = new StringBuilder(50);
        int last = args.length - 1;
        if (args[last] instanceof String) {
            args[last] = removeDelimIfPossible((String) args[last]);
        }
        for (Object arg : args) {
            sb.append(arg).append(DELIM);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static String concat2(Object... args) {
        StringBuilder sb = new StringBuilder(50);
        for (Object arg : args) {
            sb.append(arg).append(DELIM);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static String concat3(String delim, Object... args) {
        StringBuilder sb = new StringBuilder(50);
        for (Object arg : args) {
            sb.append(arg).append(delim);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static Histogram metricSnapshot2Histogram(MetricSnapshot snapshot) {
        Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir());
        List<Long> points = snapshot.get_points();
        updateHistogramPoints(histogram, points);
        return histogram;
    }

    public static Timer metricSnapshot2Timer(MetricSnapshot snapshot) {
        Timer timer = new Timer(new ExponentiallyDecayingReservoir());
        List<Long> points = snapshot.get_points();
        updateTimerPoints(timer, points);
        return timer;
    }

    public static void updateHistogramPoints(Histogram histogram, List<Long> points) {
        if (points != null) {
            for (Long pt : points) {
                histogram.update(pt);
            }
        }
    }

    public static void updateTimerPoints(Timer timer, List<Long> points) {
        if (points != null) {
            for (Long pt : points) {
                timer.update(pt, TimeUnit.MILLISECONDS);
            }
        }
    }

    public static Map<Integer, MetricSnapshot> toThriftCounterSnapshots(Map<Integer, AsmSnapshot> snapshots) {
        Map<Integer, MetricSnapshot> ret = Maps.newHashMapWithExpectedSize(snapshots.size());
        for (Map.Entry<Integer, AsmSnapshot> entry : snapshots.entrySet()) {
            ret.put(entry.getKey(), convert((AsmCounterSnapshot) entry.getValue()));
        }
        return ret;
    }

    public static Map<Integer, MetricSnapshot> toThriftGaugeSnapshots(Map<Integer, AsmSnapshot> snapshots) {
        Map<Integer, MetricSnapshot> ret = Maps.newHashMapWithExpectedSize(snapshots.size());
        for (Map.Entry<Integer, AsmSnapshot> entry : snapshots.entrySet()) {
            ret.put(entry.getKey(), convert((AsmGaugeSnapshot) entry.getValue()));
        }
        return ret;
    }

    public static Map<Integer, MetricSnapshot> toThriftMeterSnapshots(Map<Integer, AsmSnapshot> snapshots) {
        Map<Integer, MetricSnapshot> ret = Maps.newHashMapWithExpectedSize(snapshots.size());
        for (Map.Entry<Integer, AsmSnapshot> entry : snapshots.entrySet()) {
            ret.put(entry.getKey(), convert((AsmMeterSnapshot) entry.getValue()));
        }
        return ret;
    }

    public static Map<Integer, MetricSnapshot> toThriftHistoSnapshots(MetaType metaType, Map<Integer, AsmSnapshot> snapshots) {
        Map<Integer, MetricSnapshot> ret = Maps.newHashMapWithExpectedSize(snapshots.size());
        for (Map.Entry<Integer, AsmSnapshot> entry : snapshots.entrySet()) {
            MetricSnapshot histogramSnapshot = convert(metaType, (AsmHistogramSnapshot) entry.getValue());
            if (histogramSnapshot != null) {
                ret.put(entry.getKey(), histogramSnapshot);
            }
        }
        return ret;
    }

    public static Map<Integer, MetricSnapshot> toThriftTimerSnapshots(MetaType metaType, Map<Integer, AsmSnapshot> snapshots) {
        Map<Integer, MetricSnapshot> ret = Maps.newHashMapWithExpectedSize(snapshots.size());
        for (Map.Entry<Integer, AsmSnapshot> entry : snapshots.entrySet()) {
            MetricSnapshot timerSnapshot = convert(metaType, (AsmTimerSnapshot) entry.getValue());
            if (timerSnapshot != null) {
                ret.put(entry.getKey(), timerSnapshot);
            }
        }
        return ret;
    }

    public static MetricSnapshot convert(AsmCounterSnapshot snapshot) {
        MetricSnapshot ret = new MetricSnapshot();
        ret.set_metricId(snapshot.getMetricId());
        ret.set_ts(TimeUtils.alignTimeToMin(snapshot.getTs()));
        ret.set_metricType(MetricType.COUNTER.getT());
        ret.set_longValue(snapshot.getV());

        return ret;
    }

    public static MetricSnapshot convert(AsmGaugeSnapshot snapshot) {
        MetricSnapshot ret = new MetricSnapshot();
        ret.set_metricId(snapshot.getMetricId());
        ret.set_ts(TimeUtils.alignTimeToMin(snapshot.getTs()));
        ret.set_metricType(MetricType.GAUGE.getT());
        ret.set_doubleValue(snapshot.getV());

        return ret;
    }

    public static MetricSnapshot convert(AsmMeterSnapshot snapshot) {
        MetricSnapshot ret = new MetricSnapshot();
        ret.set_metricId(snapshot.getMetricId());
        ret.set_ts(TimeUtils.alignTimeToMin(snapshot.getTs()));
        ret.set_metricType(MetricType.METER.getT());

        ret.set_m1(snapshot.getM1());
        ret.set_m5(snapshot.getM5());
        ret.set_m15(snapshot.getM15());
        ret.set_mean(snapshot.getMean());

        return ret;
    }

    public static MetricSnapshot convert(MetaType metaType, AsmHistogramSnapshot snapshot) {
        // some histograms are never updated, skip such metrics
        //if (snapshot.getSnapshot().getValues().length == 0) {
        //    return null;
        //}

        MetricSnapshot ret = new MetricSnapshot();
        ret.set_metricId(snapshot.getMetricId());
        ret.set_ts(TimeUtils.alignTimeToMin(snapshot.getTs()));
        ret.set_metricType(MetricType.HISTOGRAM.getT());

        Snapshot ws = snapshot.getSnapshot();
        ret.set_min(ws.getMin());
        ret.set_max(ws.getMax());
        ret.set_p50(ws.getMedian());
        ret.set_p75(ws.get75thPercentile());
        ret.set_p95(ws.get95thPercentile());
        ret.set_p98(ws.get98thPercentile());
        ret.set_p99(ws.get99thPercentile());
        ret.set_p999(ws.get999thPercentile());
        ret.set_mean(ws.getMean());
        ret.set_stddev(ws.getStdDev());

        // only upload points for component metrics
        if (metaType == MetaType.COMPONENT || metaType == MetaType.TOPOLOGY) {
            List<Long> pts = Lists.newArrayListWithCapacity(ws.getValues().length);
            for (Long pt : ws.getValues()) {
                pts.add(pt);
            }
            ret.set_points(pts);
        } else {
            ret.set_points(new ArrayList<Long>(0));
        }

        return ret;
    }

    public static MetricSnapshot convert(MetaType metaType, AsmTimerSnapshot snapshot) {
        // some histograms are never updated, skip such metrics
        /*
        if (snapshot.getHistogram().getValues().length == 0) {
            return null;
        }
        */

        MetricSnapshot ret = new MetricSnapshot();
        ret.set_metricId(snapshot.getMetricId());
        ret.set_ts(TimeUtils.alignTimeToMin(snapshot.getTs()));
        ret.set_metricType(MetricType.TIMER.getT());

        Snapshot ws = snapshot.getHistogram();
        ret.set_min(ws.getMin());
        ret.set_max(ws.getMax());
        ret.set_p50(ws.getMedian());
        ret.set_p75(ws.get75thPercentile());
        ret.set_p95(ws.get95thPercentile());
        ret.set_p98(ws.get98thPercentile());
        ret.set_p99(ws.get99thPercentile());
        ret.set_p999(ws.get999thPercentile());
        ret.set_mean(ws.getMean());
        ret.set_stddev(ws.getStdDev());

        AsmMeterSnapshot ms = snapshot.getMeter();
        ret.set_m1(ms.getM1());
        ret.set_m15(ms.getM5());
        ret.set_m15(ms.getM15());

        // only upload points for component metrics
        if (metaType == MetaType.COMPONENT || metaType == MetaType.TOPOLOGY) {
            List<Long> pts = Lists.newArrayListWithCapacity(ws.getValues().length);
            for (Long pt : ws.getValues()) {
                pts.add(pt);
            }
            ret.set_points(pts);
        } else {
            ret.set_points(new ArrayList<Long>(0));
        }

        return ret;
    }

    public static String getMetricName(String fullName) {
        String[] parts = fullName.split(DELIM);
        return parts[parts.length - 1];
    }

    public static String str(Object obj) {
        if (obj instanceof MetricSnapshot) {
            MetricSnapshot snapshot = (MetricSnapshot) obj;
            MetricType type = MetricType.parse(snapshot.get_metricType());
            if (type == MetricType.COUNTER) {
                return counterStr(snapshot);
            } else if (type == MetricType.GAUGE) {
                return gaugeStr(snapshot);
            } else if (type == MetricType.METER) {
                return meterStr(snapshot);
            } else if (type == MetricType.HISTOGRAM) {
                return histogramStr(snapshot);
            } else if (type == MetricType.TIMER) {
                return timerStr(snapshot);
            }
        }
        return obj.toString();
    }

    public static String counterStr(MetricSnapshot snapshot) {
        StringBuilder sb = new StringBuilder(32);
        sb.append("id:").append(snapshot.get_metricId()).append(",v:").append(snapshot.get_longValue());

        return sb.toString();
    }

    public static String gaugeStr(MetricSnapshot snapshot) {
        StringBuilder sb = new StringBuilder(32);
        sb.append("id:").append(snapshot.get_metricId()).append(",v:").append(snapshot.get_doubleValue());

        return sb.toString();
    }

    public static String meterStr(MetricSnapshot snapshot) {
        StringBuilder sb = new StringBuilder(50);
        sb.append("id:").append(snapshot.get_metricId());
        sb.append(",m1:").append(snapshot.get_m1()).append(",").append("m5:").append(snapshot.get_m5())
                .append(",").append("m15:").append(snapshot.get_m15());
        return sb.toString();
    }

    public static String histogramStr(MetricSnapshot snapshot) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("histogram");
        sb.append("(").append("id:").append(snapshot.get_metricId()).append(",").append("min:").append(snapshot.get_min()).append(",").append("max:")
                .append(snapshot.get_max()).append(",").append("mean:").append(snapshot.get_mean()).append(",").append("p50:").append(snapshot.get_p50())
                .append(",").append("p75:").append(snapshot.get_p75()).append(",").append("p95:").append(snapshot.get_p95()).append(",").append("p98:")
                .append(snapshot.get_p98()).append(",").append("p99:").append(snapshot.get_p99()).append(",").append("pts:").append(snapshot.get_points_size())
                .append(")");
        return sb.toString();
    }

    public static String timerStr(MetricSnapshot snapshot) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("timer");
        sb.append("(").append("id:").append(snapshot.get_metricId()).append(",").append("min:").append(snapshot.get_min()).append(",").append("max:")
                .append(snapshot.get_max()).append(",").append("mean:").append(snapshot.get_mean()).append(",").append("p50:").append(snapshot.get_p50())
                .append(",").append("p75:").append(snapshot.get_p75()).append(",").append("p95:").append(snapshot.get_p95()).append(",").append("p98:")
                .append(snapshot.get_p98()).append(",").append("p99:").append(snapshot.get_p99()).append(",").append("m1:").append(snapshot.get_m1())
                .append(",").append("m5:").append(snapshot.get_m5()).append(",").append("m15:").append(snapshot.get_m15()).append(",").append("pts:")
                .append(snapshot.get_points_size()).append(")");
        return sb.toString();
    }

    public static void printMetricSnapshot(AsmMetric metric, Map<Integer, AsmSnapshot> snapshots) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("metric:").append(metric.getMetricName()).append(", ");
        for (Map.Entry<Integer, AsmSnapshot> entry : snapshots.entrySet()) {
            sb.append("win:").append(entry.getKey()).append(", v:")
                    .append(getSnapshotDefaultValue(entry.getValue())).append("; ");
        }

        LOG.info(sb.toString());
    }

    public static double getSnapshotDefaultValue(AsmSnapshot snapshot) {
        if (snapshot instanceof AsmCounterSnapshot) {
            return ((AsmCounterSnapshot) snapshot).getV();
        } else if (snapshot instanceof AsmGaugeSnapshot) {
            return ((AsmGaugeSnapshot) snapshot).getV();
        } else if (snapshot instanceof AsmMeterSnapshot) {
            return ((AsmMeterSnapshot) snapshot).getM1();
        } else if (snapshot instanceof AsmHistogramSnapshot) {
            return ((AsmHistogramSnapshot) snapshot).getSnapshot().getMean();
        } else if (snapshot instanceof AsmTimerSnapshot) {
            return ((AsmTimerSnapshot) snapshot).getHistogram().getMean();
        }
        return 0;
    }

    public static void printMetricInfo(MetricInfo metricInfo) {
        iterateMap(metricInfo.get_metrics());
    }

    public static void printMetricInfo(MetricInfo metricInfo, Set<String> metrics) {
        iterateMap(metricInfo.get_metrics(), metrics);
    }

    public static <T> void iterateMap(Map<String, Map<Integer, T>> map) {
        iterateMap(map, null);
    }

    public static <T> void iterateMap(Map<String, Map<Integer, T>> map, Set<String> metrics) {
        for (Map.Entry<String, Map<Integer, T>> entry : map.entrySet()) {
            String name = entry.getKey();
            boolean print = false;
            if (metrics == null) {
                print = true;
            } else {
                for (String metric : metrics) {
                    if (name.contains(metric)) {
                        print = true;
                        break;
                    }
                }
            }
            if (print) {
                Map<Integer, T> winData = entry.getValue();
                for (Map.Entry<Integer, T> win : winData.entrySet()) {
                    T v = win.getValue();
                    String str;
                    if (v instanceof MetricSnapshot) {
                        str = MetricUtils.str(v);
                    } else {
                        str = v.toString();
                    }
                    LOG.info("metric:{}, win:{}, data:{}", name, win.getKey(), str);
                }
            }
        }
    }

    private static <T> void iter(Map<String, T> map, Func func, Object... args) {
        for (Map.Entry<String, T> entry : map.entrySet()) {
            func.exec(entry, args);
        }
    }


    public interface Func {
        void exec(Object... args);
    }


    /**
     * print default value for all metrics, in the format of: name|type|value
     */
    public static void logMetrics(MetricInfo metricInfo) {
        Map<String, Map<Integer, MetricSnapshot>> metrics = metricInfo.get_metrics();
        if (metrics != null) {
            LOG.info("\nprint metrics:");
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> entry : metrics.entrySet()) {
                String name = entry.getKey();
                MetricSnapshot metricSnapshot = entry.getValue().get(AsmWindow.M1_WINDOW);
                if (metricSnapshot != null) {
                    MetricType metricType = MetricType.parse(metricSnapshot.get_metricType());
                    double v;
                    if (metricType == MetricType.COUNTER) {
                        v = metricSnapshot.get_longValue();
                    } else if (metricType == MetricType.GAUGE) {
                        v = metricSnapshot.get_doubleValue();
                    } else if (metricType == MetricType.METER) {
                        v = metricSnapshot.get_m1();
                    } else if (metricType == MetricType.HISTOGRAM) {
                        v = metricSnapshot.get_mean();
                    } else if (metricType == MetricType.TIMER) {
                        v = metricSnapshot.get_mean();
                    } else {
                        v = 0;
                    }
                    LOG.info("{}|{}|{}", metricType, v, name);
                }
            }
            LOG.info("\n");
        }
    }

    public static void main(String[] args) {
        String workerName = "WC@SequenceTest@10.1.1.0@6800@sys@Counter";
        System.out.println(worker2topologyName(workerName));
    }
}
