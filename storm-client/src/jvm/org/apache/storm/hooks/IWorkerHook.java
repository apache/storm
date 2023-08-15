/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.hooks;

import java.io.Serializable;
import java.util.Map;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.task.WorkerUserContext;

/**
 * An IWorkerHook represents a topology component that can be executed when a worker starts, and when a worker shuts down. It can be useful
 * when you want to execute operations before topology processing starts, or cleanup operations before your workers shut down.
 */
public interface IWorkerHook extends Serializable {
    /**
     * This method is called when a worker is started.
     *
     * @param topoConf The Storm configuration for this worker
     * @param context  This object can be used to get information about this worker's place within the topology
     *
     * @deprecated see {@link IWorkerHook#start(Map, WorkerUserContext)}
     */
    default void start(Map<String, Object> topoConf, WorkerTopologyContext context) {
        // NOOP
    }

    /**
     * This method is called when a worker is started and can be used to do necessary prep-processing and allow initialization of shared
     * application state.
     *
     * @param topoConf The Storm configuration for this worker
     * @param context  This object can be used to get information about this worker's place within the topology and exposes
     * {@link WorkerUserContext#setResource(String, Object)} to set the shared application state.
     */
    default void start(Map<String, Object> topoConf, WorkerUserContext context) {
        start(topoConf, context);
    }

    /**
     * This method is called right before a worker shuts down.
     */
    void shutdown();
}
