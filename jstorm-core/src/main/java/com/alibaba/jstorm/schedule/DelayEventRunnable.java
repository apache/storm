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
package com.alibaba.jstorm.schedule;

import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.NimbusUtils;
import com.alibaba.jstorm.daemon.nimbus.StatusType;

public class DelayEventRunnable implements Runnable {

    private NimbusData data;
    private String topologyid;
    private StatusType status;
    private Object[] args;

    public DelayEventRunnable(NimbusData data, String topologyid,
            StatusType status, Object[] args) {
        this.data = data;
        this.topologyid = topologyid;
        this.status = status;
        this.args = args;
    }

    @Override
    public void run() {
        NimbusUtils.transition(data, topologyid, false, status, args);
    }

}
