/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kinesis.spout;

import org.apache.storm.kinesis.spout.exceptions.InvalidSeekPositionException;

/**
 * Fetches data from a shard.
 */
public interface IShardGetter {
    /**
     * Get at most maxNumberOfRecords.
     *
     * @param maxNumberOfRecords Max number of records to get.
     * @return data records
     */
    Records getNext(int maxNumberOfRecords);

    /**
     * Seek to a position in the shard. All subsequent calls to getNext will read starting
     * from the specified position.
     * Note: This is expected to be invoked infrequently (e.g. some implementations may not support a high call rate).
     *
     * @param position  Position to seek to.
     * @throws InvalidSeekPositionException if the position is invalid (e.g. sequence number not valid for the shard).
     */
    void seek(ShardPosition position) throws InvalidSeekPositionException;

    /**
     * @return shardId of the shard this getter reads from.
     */
    String getAssociatedShard();
}
