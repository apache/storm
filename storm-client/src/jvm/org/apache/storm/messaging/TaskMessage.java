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

package org.apache.storm.messaging;

import java.nio.ByteBuffer;

public class TaskMessage {
    private int task;
    private byte[] message;

    public TaskMessage(int task, byte[] message) {
        this.task = task;
        this.message = message;
    }

    public int task() {
        return task;
    }

    public byte[] message() {
        return message;
    }

    public ByteBuffer serialize() {
        ByteBuffer bb = ByteBuffer.allocate(message.length + 2);
        bb.putShort((short) task);
        bb.put(message);
        return bb;
    }

    public void deserialize(ByteBuffer packet) {
        if (packet == null) {
            return;
        }
        task = packet.getShort();
        message = new byte[packet.limit() - 2];
        packet.get(message);
    }

}
