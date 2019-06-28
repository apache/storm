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

package org.apache.storm.streams;

import org.apache.storm.tuple.Tuple;

/**
 * Provides reference counting of tuples. Used when operations that operate on a batch of tuples are involved (e.g. aggregation, join etc).
 * The input tuples are acked once the result is emitted downstream.
 */
public class RefCountedTuple {
    private final Tuple tuple;
    private int count = 0;
    private boolean acked;

    RefCountedTuple(Tuple tuple) {
        this.tuple = tuple;
        this.acked = false;
    }

    public boolean shouldAck() {
        return count == 0 && !acked;
    }

    public void increment() {
        ++count;
    }

    public void decrement() {
        --count;
    }

    public Tuple tuple() {
        return tuple;
    }

    public void setAcked() {
        acked = true;
    }

    @Override
    public String toString() {
        return "RefCountedTuple{"
                + "count=" + count
                + ", tuple=" + tuple
                + '}';
    }
}
