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
import org.apache.storm.utils.Utils;

/**
 * A request for a shared memory region on heap.
 */
public class SharedOnHeap extends SharedMemory {
    private static final long serialVersionUID = 1L;

    /**
     * Create a new request for a shared memory region on heap.
     *
     * @param amount the number of MB to share on heap
     * @param name   the name of the shared region (for tracking purposes)
     */
    public SharedOnHeap(double amount, String name) {
        super(name == null ? Utils.uuid() : name);
        set_on_heap(amount);
    }
}
