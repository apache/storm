package com.alibaba.jstorm.cluster;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Topology metrics monitor in ZK
 */

public class StormMonitor implements Serializable {
	private static final long serialVersionUID = -6023196346496305314L;
	private boolean metricsMonitor;

	public StormMonitor(boolean metricsMonitor) {
		this.metricsMonitor = metricsMonitor;
	}
	
	public void setMetrics(boolean metrics) {
		this.metricsMonitor = metrics;
	}
	
    public boolean getMetrics() {
    	return this.metricsMonitor;
    }
    
    @Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}