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
package com.alibaba.jstorm.metric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;

public class JStormHealthCheck {
    private static final Logger LOG = LoggerFactory.getLogger(JStormHealthCheck.class);

    private final static Map<Integer, HealthCheckRegistry> taskHealthCheckMap = new ConcurrentHashMap<Integer, HealthCheckRegistry>();

    private final static HealthCheckRegistry workerHealthCheck = new HealthCheckRegistry();

    public static void registerTaskHealthCheck(int taskId, String name, HealthCheck healthCheck) {
        HealthCheckRegistry healthCheckRegister = taskHealthCheckMap.get(taskId);

        if (healthCheckRegister == null) {
            healthCheckRegister = new HealthCheckRegistry();
            taskHealthCheckMap.put(taskId, healthCheckRegister);
        }

        healthCheckRegister.register(name, healthCheck);
    }

    public static void registerWorkerHealthCheck(String name, HealthCheck healthCheck) {
        workerHealthCheck.register(name, healthCheck);
    }

    public static void unregisterTaskHealthCheck(int taskId, String name) {
        HealthCheckRegistry healthCheckRegister = taskHealthCheckMap.get(taskId);

        if (healthCheckRegister != null) {
            healthCheckRegister.unregister(name);
        }

    }

    public static void unregisterWorkerHealthCheck(String name) {
        workerHealthCheck.unregister(name);
    }

    public static Map<Integer, HealthCheckRegistry> getTaskhealthcheckmap() {
        return taskHealthCheckMap;
    }

    public static HealthCheckRegistry getWorkerhealthcheck() {
        return workerHealthCheck;
    }

}
