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
package com.alibaba.jstorm.callback;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;

/**
 * AsyncLoopThread 's runnable
 * 
 * The class wrapper RunnableCallback fn, if occur exception, run killfn
 * 
 * @author yannian
 * 
 */
public class AsyncLoopRunnable implements Runnable {
    private static Logger LOG = LoggerFactory
            .getLogger(AsyncLoopRunnable.class);

    // set shutdown as false is to
    private static AtomicBoolean shutdown = new AtomicBoolean(false);

    public static AtomicBoolean getShutdown() {
        return shutdown;
    }

    private RunnableCallback fn;
    private RunnableCallback killfn;
    private long lastTime = System.currentTimeMillis();

    public AsyncLoopRunnable(RunnableCallback fn, RunnableCallback killfn) {
        this.fn = fn;
        this.killfn = killfn;
    }

    private boolean needQuit(Object rtn) {
        if (rtn != null) {
            long sleepTime = Long.parseLong(String.valueOf(rtn));
            if (sleepTime < 0) {
                return true;
            } else if (sleepTime > 0) {
                long now = System.currentTimeMillis();
                long cost = now - lastTime;
                long sleepMs = sleepTime * 1000 - cost;
                if (sleepMs > 0) {
                    JStormUtils.sleepMs(sleepMs);
                    lastTime = System.currentTimeMillis();
                } else {
                    lastTime = now;
                }

            }
        }
        return false;
    }

    private void shutdown() {
        fn.postRun();
        fn.shutdown();
        LOG.info("Succefully shutdown");
    }

    @Override
    public void run() {

        if (fn == null) {
            LOG.error("fn==null");
            throw new RuntimeException("AsyncLoopRunnable no core function ");
        }

        fn.preRun();

        try {
            while (shutdown.get() == false) {
                Exception e = null;

                fn.run();

                if (shutdown.get() == true) {
                    shutdown();
                    return;
                }

                e = fn.error();
                if (e != null) {
                    throw e;
                }
                Object rtn = fn.getResult();
                if (this.needQuit(rtn)) {
                    shutdown();
                    return;
                }

            }
        } catch (Throwable e) {
            if (shutdown.get() == true) {
                shutdown();
                return;
            } else {
                LOG.error("Async loop died!!!" + e.getMessage(), e);
                killfn.execute(e);
            }

        }

    }

}
