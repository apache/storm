package com.alibaba.jstorm.metric;

import com.alibaba.jstorm.common.metric.MetricMeta;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public interface MetaFilter {
    boolean matches(MetricMeta meta, Object arg);
}
