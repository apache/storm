package com.alibaba.jstorm.daemon.worker.metrics;

import java.util.Map;
import java.util.List;

public class MetricSendClient {
	
	public MetricSendClient() {	
	}
	
	public boolean send(Map<String, Object> msg) {
		return true;
	}
	
	public boolean send(List<Map<String, Object>> msgList) {
		return true;
	}
}