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

import com.lmax.disruptor.RingBuffer;

public class JstormProducer implements Runnable {

    Logger logger = LoggerFactory.getLogger(JstormProducer.class);

    private RingBuffer<JstormEvent> ringBuffer;
    private int size;

    public JstormProducer(RingBuffer<JstormEvent> ringBuffer, int size) {
        this.ringBuffer = ringBuffer;
        this.size = size;
    }

    @Override
    public void run() {
        logger.warn("producer start..." + System.currentTimeMillis());

        // while (true) {
        // long seqId = ringBuffer.next();
        //
        // ringBuffer.get(seqId).setMsgId(String.valueOf(seqId));
        // ringBuffer.publish(seqId);
        //
        // try {
        // double random = Math.random();
        // Thread.sleep((long)(random * 1000));
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        // }

        for (int i = 0; i < size; i++) {
            long seqId = ringBuffer.next();

            ringBuffer.get(seqId).setMsgId(String.valueOf(seqId));
            ringBuffer.publish(seqId);
        }
    }

}
