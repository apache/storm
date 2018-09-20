/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.nimbus;

import java.util.Map;
import org.apache.storm.DaemonConfig;
import org.apache.storm.shade.com.google.common.base.Preconditions;
import org.apache.storm.utils.ReflectionUtils;

/**
 * Factory class for recovery strategy.
 */
public class WorkerHeartbeatsRecoveryStrategyFactory {

    /**
     * Get instance of {@link IWorkerHeartbeatsRecoveryStrategy} with conf.
     * @param conf strategy config
     * @return an instance of {@link IWorkerHeartbeatsRecoveryStrategy}
     */
    public static IWorkerHeartbeatsRecoveryStrategy getStrategy(Map<String, Object> conf) {
        IWorkerHeartbeatsRecoveryStrategy strategy;
        if (conf.get(DaemonConfig.NIMBUS_WORKER_HEARTBEATS_RECOVERY_STRATEGY_CLASS) != null) {
            Object targetObj = ReflectionUtils.newInstance((String)
                                                               conf.get(DaemonConfig.NIMBUS_WORKER_HEARTBEATS_RECOVERY_STRATEGY_CLASS));
            Preconditions.checkState(targetObj instanceof IWorkerHeartbeatsRecoveryStrategy,
                                     "{} must implements IWorkerHeartbeatsRecoveryStrategy",
                                     DaemonConfig.NIMBUS_WORKER_HEARTBEATS_RECOVERY_STRATEGY_CLASS);
            strategy = ((IWorkerHeartbeatsRecoveryStrategy) targetObj);
        } else {
            strategy = new TimeOutWorkerHeartbeatsRecoveryStrategy();
        }

        strategy.prepare(conf);
        return strategy;
    }

}
