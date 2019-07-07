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

import org.apache.storm.generated.SharedMemory;

/**
 * This is a new base interface that can be used by anything that wants to mirror RAS's basic API. Trident uses this to allow setting
 * resources in the Stream API.
 */
public interface ResourceDeclarer<T extends ResourceDeclarer> {
    /**
     * Set the amount of on heap memory for this component.
     *
     * @param onHeap the amount of on heap memory
     * @return this for chaining
     */
    T setMemoryLoad(Number onHeap);

    /**
     * Set the amount of memory for this component on and off heap.
     *
     * @param onHeap  the amount of on heap memory
     * @param offHeap the amount of off heap memory
     * @return this for chaining
     */
    T setMemoryLoad(Number onHeap, Number offHeap);

    /**
     * Set the amount of CPU load for this component.
     *
     * @param amount the amount of CPU
     * @return this for chaining
     */
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    T setCPULoad(Number amount);

    /**
     * Add in request for shared memory that this component will use. See {@link SharedOnHeap}, {@link SharedOffHeapWithinNode}, and {@link
     * SharedOffHeapWithinWorker} for convenient ways to create shared memory requests.
     *
     * @param request the shared memory request for this component
     * @return this for chaining
     */
    T addSharedMemory(SharedMemory request);
}
