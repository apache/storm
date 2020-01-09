/*
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

package org.apache.storm.daemon.supervisor.timer;

import java.util.Map;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.healthcheck.HealthChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SupervisorHealthCheck implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorHealthCheck.class);
    private final Supervisor supervisor;

    public SupervisorHealthCheck(Supervisor supervisor) {
        this.supervisor = supervisor;
    }

    @Override
    public void run() {
        Map<String, Object> conf = supervisor.getConf();
        LOG.info("Running supervisor healthchecks...");
        int healthCode = HealthChecker.healthCheck(conf, supervisor.getMetricsRegistry());
        if (healthCode != 0) {
            LOG.info("The supervisor healthchecks FAILED...");
            supervisor.shutdownAllWorkers(null, null);
            throw new RuntimeException("Supervisor failed health check. Exiting.");
        }
    }
}
