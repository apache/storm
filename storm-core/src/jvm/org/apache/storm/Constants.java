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
package org.apache.storm;

import org.apache.storm.coordination.CoordinatedBolt;

import java.util.Arrays;
import java.util.List;


public class Constants {
    public static final String COORDINATED_STREAM_ID = CoordinatedBolt.class.getName() + "/coord-stream";

    public static final long SYSTEM_TASK_ID = -1;
    public static final List<Long> SYSTEM_EXECUTOR_ID = Arrays.asList(-1L, -1L);
    public static final String SYSTEM_COMPONENT_ID = "__system";
    public static final String SYSTEM_TICK_STREAM_ID = "__tick";
    public static final String METRICS_COMPONENT_ID_PREFIX = "__metrics";
    public static final String METRICS_STREAM_ID = "__metrics";
    public static final String METRICS_TICK_STREAM_ID = "__metrics_tick";
    public static final String CREDENTIALS_CHANGED_STREAM_ID = "__credentials";

    public static final Object TOPOLOGY = "topology";
    public static final String SYSTEM_TOPOLOGY = "system-topology";
    public static final String STORM_CONF = "storm-conf";
    public static final String STORM_ID = "storm-id";
    public static final String WORKER_ID = "worker-id";
    public static final String CONF = "conf";
    public static final String PORT = "port";
    public static final String TASK_TO_COMPONENT = "task->component";
    public static final String COMPONENT_TO_SORTED_TASKS = "component->sorted-tasks";
    public static final String COMPONENT_TO_STREAM_TO_FIELDS = "component->stream->fields";
    public static final String TASK_IDS = "task-ids";
    public static final String DEFAULT_SHARED_RESOURCES = "default-shared-resources";
    public static final String USER_SHARED_RESOURCES = "user-shared-resources";
    public static final String USER_TIMER = "user-timer";
    public static final String TRANSFER_FN = "transfer-fn";
    public static final String SUICIDE_FN = "suicide-fn";
    public static final String THROTTLE_ON = "throttle-on";
    public static final String EXECUTOR_RECEIVE_QUEUE_MAP = "executor-receive-queue-map";
    public static final String STORM_ACTIVE_ATOM = "storm-active-atom";
    public static final String COMPONENT_TO_DEBUG_ATOM = "storm-component->debug-atom";
    public static final Object LOAD_MAPPING = "load-mapping";
}
    
