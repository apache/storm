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

import backtype.storm.topology.IBasicBolt;

public interface IBatchSpout extends IBasicBolt, ICommitter, Serializable {

    /**
     * input's filed 0 is BatchId
     * 
     * execute only receive trigger message
     * 
     * do emitBatch operation in execute whose streamID is
     * "batch/compute-stream"
     */
    // void execute(Tuple input, IBasicOutputCollector collector);
    /**
     * begin to ack batchId's data
     * 
     * return value will be stored in ZK, so sometimes don't need special action
     * 
     * @param id
     */
    // byte[] commit(BatchId id) throws FailedException;

    /**
     * begin to revert batchId's data
     * 
     * If current task fails to commit batchId, it won't call revert(batchId) If
     * current task fails to revert batchId, JStorm won't call revert again.
     * 
     * if not transaction, it can don't care revert
     * 
     * @param id
     */
    // void revert(BatchId id, byte[] commitResult);
}
