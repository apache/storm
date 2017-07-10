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

package org.apache.storm.rocketmq;

/**
 * Interface for messages retry manager.
 */
public interface MessageRetryManager {
    /**
     * Remove from the cache. Message with the id is successful.
     * @param id message id
     */
    void ack(String id);

    /**
     * Remove from the cache. Message with the id is failed.
     * Invoke retry logics if necessary.
     * @param id message id
     */
    void fail(String id);

    /**
     * Mark message in the cache.
     * @param message message
     */
    void mark(ConsumerMessage message);

    /**
     * Whether the message need retry.
     * @param message ConsumerMessage
     * @return true if need retry, otherwise false
     */
    boolean needRetry(ConsumerMessage message);

}
