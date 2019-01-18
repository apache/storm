/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.messaging.netty;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.shade.io.netty.buffer.ByteBuf;
import org.apache.storm.shade.io.netty.buffer.ByteBufAllocator;

// Instances of this type are sent from NettyWorker to upstream WorkerTransfer to indicate BackPressure situation
public class BackPressureStatus {
    public static final short IDENTIFIER = (short) -600;
    private static final int SIZE_OF_ID = 2; // size if IDENTIFIER
    private static final int SIZE_OF_INT = 4;

    private static AtomicLong bpCount = new AtomicLong(0);
    public final long id;                  // monotonically increasing id for correlating sent/recvd msgs. ok if restarts from 0 on crash.
    public String workerId;
    public Collection<Integer> bpTasks;    // task Ids experiencing BP. can be null
    public Collection<Integer> nonBpTasks; // task Ids no longer experiencing BP. can be null

    public BackPressureStatus() {
        this.id = bpCount.incrementAndGet();
    }

    /**
     * Constructor.
     */
    public BackPressureStatus(String workerId, Collection<Integer> bpTasks, Collection<Integer> nonBpTasks) {
        this.workerId = workerId;
        this.id = bpCount.incrementAndGet();
        this.bpTasks = bpTasks;
        this.nonBpTasks = nonBpTasks;
    }

    public static BackPressureStatus read(byte[] bytes, KryoValuesDeserializer deserializer) {
        return (BackPressureStatus) deserializer.deserializeObject(bytes);
    }

    @Override
    public String toString() {
        return "{worker=" + workerId + ", bpStatusId=" + id + ", bpTasks=" + bpTasks + ", nonBpTasks=" + nonBpTasks + '}';
    }

    /**
     * Encoded as -600 ... short(2) len ... int(4) payload ... byte[]     *
     */
    public ByteBuf buffer(ByteBufAllocator alloc, KryoValuesSerializer ser) throws IOException {
        byte[] serializedBytes = ser.serializeObject(this);
        ByteBuf buff = alloc.ioBuffer(SIZE_OF_ID + SIZE_OF_INT + serializedBytes.length);
        buff.writeShort(IDENTIFIER);
        buff.writeInt(serializedBytes.length);
        buff.writeBytes(serializedBytes);
        return buff;
    }
}
