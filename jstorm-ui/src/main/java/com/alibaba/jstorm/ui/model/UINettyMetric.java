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

import com.alibaba.jstorm.metric.MetricDef;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UINettyMetric extends UIBasicMetric {
    private String connection;
    private String fromWorker;
    private String toWorker;
    private boolean isFrom = false;
    private boolean isTo = false;

    public static final String[] HEAD = {MetricDef.NETTY_SRV_MSG_TRANS_TIME};

    public UINettyMetric(String host, String connection) {
        this.connection = connection;
        this.fromWorker = connection.split("->")[0];
        this.toWorker = connection.split("->")[1];
        if (fromWorker.contains(host)) {
            isFrom = true;
        }
        if (toWorker.contains(host)) {
            isTo = true;
        }
    }


    public String getConnection() {
        return connection;
    }

    public String getFromWorker() {
        return fromWorker;
    }

    public String getToWorker() {
        return toWorker;
    }

    public boolean isFrom() {
        return isFrom;
    }

    public boolean isTo() {
        return isTo;
    }
}
