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

package org.apache.storm.topology;

import org.apache.storm.state.State;

/**
 * A windowed bolt abstraction for supporting windowing operation with state.
 */
public interface IStatefulWindowedBolt<T extends State> extends IStatefulComponent<T>, IWindowedBolt {
    /**
     * If the stateful windowed bolt should have its windows persisted in state and maintain a subset of events in memory.
     * <p>
     * The default is to keep all the window events in memory.
     * </p>
     *
     * @return true if the windows should be persisted
     */
    default boolean isPersistent() {
        return false;
    }

    /**
     * The maximum number of window events to keep in memory.
     */
    default long maxEventsInMemory() {
        return 1_000_000L; // default
    }
}
