package com.alibaba.jstorm.common.metric;

import com.alibaba.jstorm.daemon.nimbus.TopologyMetricsRunnable;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wange
 * @since 15/7/14
 */
public class MetricMetaParser {
    private static final Logger logger = LoggerFactory.getLogger(TopologyMetricsRunnable.class);

    public static MetricMeta fromMetricName(String name) {
        try {
            String[] parts = name.split(MetricUtils.DELIM);
            char ch = parts[0].charAt(0);
            if (ch == 'W' || ch == 'N' || ch == 'P') {
                return parseWorkerMetricMeta(parts);
            } else {
                return parseTaskMetricMeta(parts);
            }
        } catch (Exception ex) {
            logger.error("Error parsing metric meta, name:{}", name, ex);
        }
        return null;
    }

    private static MetricMeta parseTaskMetricMeta(String[] parts) {
        MetricMeta meta = new MetricMeta();
        meta.setMetaType(MetaType.parse(parts[0].charAt(0)).getT());
        meta.setMetricType(MetricType.parse(parts[0].charAt(1)).getT());
        meta.setTopologyId(parts[1]);
        meta.setComponent(parts[2]);
        meta.setTaskId(Integer.valueOf(parts[3]));
        meta.setStreamId(parts[4]);
        meta.setMetricGroup(parts[5]);
        meta.setMetricName(parts[6]);

        return meta;
    }

    private static MetricMeta parseWorkerMetricMeta(String[] parts) {
        MetricMeta meta = new MetricMeta();
        meta.setMetaType(MetaType.parse(parts[0].charAt(0)).getT());
        meta.setMetricType(MetricType.parse(parts[0].charAt(1)).getT());
        meta.setTopologyId(parts[1]);
        meta.setHost(parts[2]);
        meta.setPort(Integer.valueOf(parts[3]));
        meta.setMetricGroup(parts[4]);
        meta.setMetricName(parts[5]);

        return meta;
    }
}
