package com.alibaba.jstorm.metric;

import java.util.List;
import java.util.Map;

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