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
package com.alibaba.jstorm.message.netty;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.RunnableCallback;

public class ReconnectRunnable extends RunnableCallback {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReconnectRunnable.class);

    private BlockingQueue<NettyClient> queue =
            new LinkedBlockingDeque<NettyClient>();

    public void pushEvent(NettyClient client) {
        queue.offer(client);
    }

    private boolean closed = false;
    private Thread thread = null;

    @Override
    public void run() {
        LOG.info("Successfully start reconnect thread");
        thread = Thread.currentThread();
        while (closed == false) {
            NettyClient client = null;
            try {
                client = queue.take();
            } catch (InterruptedException e) {
                continue;
            }
            if (client != null) {
                client.doReconnect();
            }

        }

        LOG.info("Successfully shutdown reconnect thread");
    }

    @Override
    public void shutdown() {
        closed = true;
        thread.interrupt();
    }

    @Override
    public Object getResult() {
        return -1;
    }
}
