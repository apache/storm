package com.alibaba.jstorm.metric;

import java.util.UUID;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class DefaultMetricIDGenerator implements MetricIDGenerator {

    @Override
    public long genMetricId(String metricName) {
        return UUID.randomUUID().getLeastSignificantBits();
    }
}
