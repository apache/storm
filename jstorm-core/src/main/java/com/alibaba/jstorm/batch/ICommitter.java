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
package com.alibaba.jstorm.batch;

import java.io.Serializable;

import backtype.storm.topology.FailedException;

/**
 * The less committer, the state is more stable. Don't need to do
 * 
 * @author zhongyan.feng
 * @version
 */
public interface ICommitter extends Serializable {
    /**
     * begin to commit batchId's data, then return the commit result The
     * commitResult will store into outside storage
     * 
     * if failed to commit, please throw FailedException
     * 
     * 
     * 
     * @param id
     */
    byte[] commit(BatchId id) throws FailedException;

    /**
     * begin to revert batchId's data
     * 
     * If current task fails to commit batchId, it won't call revert(batchId) If
     * current task fails to revert batchId, JStorm won't call revert again.
     * 
     * @param id
     */
    void revert(BatchId id, byte[] commitResult);
}
