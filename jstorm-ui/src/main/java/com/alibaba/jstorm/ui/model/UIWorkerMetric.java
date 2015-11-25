/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.ui.model;

import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UIWorkerMetric extends UIBasicMetric {
    private String host;
    private String port;
    private String topology;

    public static final String[] HEAD = {MetricDef.CPU_USED_RATIO, MetricDef.DISK_USAGE, MetricDef.MEMORY_USED,
            MetricDef.NETTY_CLI_SEND_SPEED, MetricDef.NETTY_SRV_RECV_SPEED, MetricDef.NETWORK_MSG_DECODE_TIME};

    public UIWorkerMetric(String _host, String _port) {
        host = _host;
        port = _port;
    }

    public UIWorkerMetric(String _host, String _port, String _topology) {
        host = _host;
        port = _port;
        topology = _topology;
    }

    public void setMetricValue(MetricSnapshot snapshot, String metricName) {
        if (metricName.equals(MetricDef.MEMORY_USED)) {
            String value = (long) snapshot.get_doubleValue() + "";
            setValue(metricName, value);

        } else {
            String value = UIMetricUtils.getMetricValue(snapshot);
            setValue(metricName, value);
        }
    }


    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getTopology() {
        return topology;
    }

    public void setTopology(String topology) {
        this.topology = topology;
    }
}
