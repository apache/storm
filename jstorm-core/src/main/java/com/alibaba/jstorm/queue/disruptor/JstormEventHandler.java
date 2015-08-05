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
package com.alibaba.jstorm.queue.disruptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventHandler;

public class JstormEventHandler implements EventHandler {

    Logger logger = LoggerFactory.getLogger(JstormEventHandler.class);

    private int count;

    public JstormEventHandler(int count) {
        this.count = count;
    }

    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch)
            throws Exception {
        long msgId = Long.parseLong(((JstormEvent) event).getMsgId());
        // if (msgId % size ==0) {
        // logger.warn("consumer msgId=" + msgId + ", seq=" + sequence);
        // }
        if (msgId == count - 1) {
            logger.warn("end..." + System.currentTimeMillis());
        }
    }

}
