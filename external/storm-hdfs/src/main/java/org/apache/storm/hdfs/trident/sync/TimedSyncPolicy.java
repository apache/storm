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
package org.apache.storm.hdfs.trident.sync;

import org.apache.storm.trident.tuple.TridentTuple;

/**
 * SyncPolicy implementation that flush to HDFS periodically.
 */
public class TimedSyncPolicy implements SyncPolicy {
    private final long interval;
    private long lastPoint;

    public TimedSyncPolicy(long interval) {
        this.interval = interval;
        this.lastPoint = System.currentTimeMillis();
    }

    @Override
    public boolean mark(TridentTuple tuple, long offset) {
        long currentPoint = System.currentTimeMillis();
        if ((currentPoint - lastPoint) > interval) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void reset() {
        this.lastPoint = System.currentTimeMillis();
    }
}
