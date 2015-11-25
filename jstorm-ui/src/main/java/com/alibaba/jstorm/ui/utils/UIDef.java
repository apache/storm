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
package com.alibaba.jstorm.ui.utils;


import com.alibaba.jstorm.metric.MetricDef;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UIDef {
    public static final String API_V1 = "/api/v1";
    public static final String APP_NAME = "JStorm";
    public static final String DEFAULT_CLUSTER_NAME = "default";
    public static final int PAGE_MAX = 15;

    public static final Map<String, String> HEAD_MAP = new HashMap<String, String>() {{
        put(MetricDef.EMMITTED_NUM, MetricDef.EMMITTED_NUM);
        put(MetricDef.ACKED_NUM, MetricDef.ACKED_NUM);
        put(MetricDef.FAILED_NUM, MetricDef.FAILED_NUM);
        put(MetricDef.SEND_TPS, MetricDef.SEND_TPS);
        put(MetricDef.RECV_TPS, MetricDef.RECV_TPS);
        put(MetricDef.PROCESS_LATENCY, "Process(us)");
        put(MetricDef.DESERIALIZE_TIME, "Deser(us)");
        put(MetricDef.SERIALIZE_TIME, "Ser(us)");
        put(MetricDef.EXECUTE_TIME, "Exe(us)");
        put(MetricDef.COLLECTOR_EMIT_TIME, "Emit(us)");
        put(MetricDef.ACKER_TIME, "Acker(us)");
        put(MetricDef.TUPLE_LIEF_CYCLE, "TupleLifeCycle(us)");
        put(MetricDef.SERIALIZE_QUEUE, "Ser(%)");
        put(MetricDef.DESERIALIZE_QUEUE, "Deser(%)");
        put(MetricDef.EXECUTE_QUEUE, "Exe(%)");
        put(MetricDef.CPU_USED_RATIO, "CPU Used(%)");
        put(MetricDef.DISK_USAGE, "Disk Used(%)");
        put(MetricDef.MEMORY_USED, "Mem Used");
        put(MetricDef.EXECUTE_QUEUE, "ExeQueue(%)");
        put(MetricDef.DESERIALIZE_QUEUE, "DeserQueue(%)");
        put(MetricDef.SERIALIZE_QUEUE, "SerQueue(%)");
        put(MetricDef.NETWORK_MSG_DECODE_TIME, "Network Msg Decode Time(us)");
    }};
}