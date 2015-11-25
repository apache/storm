package com.alibaba.jstorm.metric;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public interface MetricIDGenerator {
    long genMetricId(String metricName);
}
