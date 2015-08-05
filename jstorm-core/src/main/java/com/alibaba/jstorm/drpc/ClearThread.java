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
package com.alibaba.jstorm.drpc;

import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.DRPCExecutionException;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

public class ClearThread extends RunnableCallback {
    private static final Logger LOG = LoggerFactory
            .getLogger(ClearThread.class);

    private final int REQUEST_TIMEOUT_SECS;
    private static final int TIMEOUT_CHECK_SECS = 5;

    private Drpc drpcService;

    public ClearThread(Drpc drpc) {
        drpcService = drpc;

        REQUEST_TIMEOUT_SECS =
                JStormUtils.parseInt(
                        drpcService.getConf().get(
                                Config.DRPC_REQUEST_TIMEOUT_SECS), 60);
        LOG.info("Drpc timeout seconds is " + REQUEST_TIMEOUT_SECS);
    }

    @Override
    public void run() {

        for (Entry<String, Integer> e : drpcService.getIdtoStart().entrySet()) {
            if (TimeUtils.time_delta(e.getValue()) > REQUEST_TIMEOUT_SECS) {
                String id = e.getKey();

                drpcService.getIdtoResult().put(id,
                        new DRPCExecutionException("Request timed out"));
                Semaphore s = drpcService.getIdtoSem().get(id);
                if (s != null) {
                    s.release();
                }
                drpcService.cleanup(id);
                LOG.info("Clear request " + id);
            }
        }

        JStormUtils.sleepMs(10);

    }

    public Object getResult() {
        return TIMEOUT_CHECK_SECS;

    }

}
