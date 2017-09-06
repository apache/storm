/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.executor;

import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.RegisteredGlobalState;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalExecutor {

    private static volatile String trackId = null;

    public static Executor mkExecutor(WorkerState workerState, List<Long> executorId, Map<String, String> initialCredentials)
        throws Exception {
        Executor executor = Executor.mkExecutor(workerState, executorId, initialCredentials);
        executor.setLocalExecutorTransfer(new ExecutorTransfer(workerState, executor.getStormConf()) {
            @Override
            public void transfer(AddressedTuple tuple) throws InterruptedException {
                if (null != trackId) {
                    ((AtomicInteger) ((Map) RegisteredGlobalState.getState(trackId)).get("transferred")).incrementAndGet();
                }
                super.transfer(tuple);
            }
        });
        return executor;
    }

    public static void setTrackId(String trackId) {
        LocalExecutor.trackId = trackId;
    }

    public static void clearTrackId() {
        LocalExecutor.trackId = null;
    }
}
