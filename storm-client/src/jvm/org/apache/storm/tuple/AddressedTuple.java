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

package org.apache.storm.tuple;

import org.apache.storm.Constants;
import org.apache.storm.task.GeneralTopologyContext;

/**
 * A Tuple that is addressed to a destination.
 */
public final class AddressedTuple {
    /**
     * Destination used when broadcasting a tuple.
     */
    public static final int BROADCAST_DEST = -2;
    public final Tuple tuple;
    public final int dest;

    public AddressedTuple(int dest, Tuple tuple) {
        this.dest = dest;
        this.tuple = tuple;
    }

    public static AddressedTuple createFlushTuple(GeneralTopologyContext workerTopologyContext) {
        TupleImpl tuple = new TupleImpl(workerTopologyContext, new Values(), Constants.SYSTEM_COMPONENT_ID,
                                        (int) Constants.SYSTEM_TASK_ID, Constants.SYSTEM_FLUSH_STREAM_ID);
        return new AddressedTuple(AddressedTuple.BROADCAST_DEST, tuple); // one instance per executor avoids false sharing of CPU cache
    }

    public Tuple getTuple() {
        return tuple;
    }

    public int getDest() {
        return dest;
    }

    @Override
    public String toString() {
        return "[dest: " + dest + " tuple: " + tuple + "]";
    }
}
