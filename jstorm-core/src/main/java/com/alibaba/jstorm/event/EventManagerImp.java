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
package com.alibaba.jstorm.event;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.RunnableCallback;

/**
 * Event Manager, drop one event from queue, then execute the event.
 */
public class EventManagerImp extends RunnableCallback implements EventManager {
    private static final Logger LOG = LoggerFactory
            .getLogger(EventManagerImp.class);

    private AtomicInteger added = new AtomicInteger();
    private AtomicInteger processed = new AtomicInteger();

    private LinkedBlockingQueue<RunnableCallback> queue =
            new LinkedBlockingQueue<RunnableCallback>();

    private Exception e;

    public void proccessinc() {
        processed.incrementAndGet();
    }

    @Override
    public void add(RunnableCallback event_fn) {
        added.incrementAndGet();
        queue.add(event_fn);
    }

    @Override
    public boolean waiting() {
        return (processed.get() == added.get());

    }

    @Override
    public Exception error() {
        return e;
    }

    @Override
    public void run() {
        try {
            RunnableCallback r = queue.take();
            if (r == null) {

                return;
            }

            r.run();

            e = r.error();

            proccessinc();

        } catch (InterruptedException e) {
            LOG.info("InterruptedException when processing event ");
        }

    }
}
