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

package org.apache.storm.kinesis.spout.exceptions;

import org.apache.storm.kinesis.spout.ShardPosition;

/**
 * Thrown when the specified seek position in the shard is invalid.
 * E.g. specified sequence number is invalid for this shard/stream.
 */
public class InvalidSeekPositionException extends Exception {
    private static final long serialVersionUID = 6965780387493774707L;

    private final ShardPosition position;

    /**
     * @param position Shard position which we could not seek to.
     */
    public InvalidSeekPositionException(ShardPosition position) {
        super("Invalid seek position " + position);
        this.position = position;
    }

    /**
     * @return shard position.
     */
    public ShardPosition getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "Cannot seek to position " + position;
    }
}
