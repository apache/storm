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
package backtype.storm.messaging;

import java.nio.ByteBuffer;

public class TaskMessage {
    private int _task;
    private int _taskSrc;
    private byte[] _message;
    
    public TaskMessage(int task, byte[] message) {
        _task = task;
        _message = message;
    }

    public TaskMessage(int task, int taskSrc, byte[] message) {
        _task = task;
        _taskSrc = taskSrc;
        _message = message;
    }
    
    public int task() {
        return _task;
    }

    public int taskSrc() {
        return _taskSrc;
    }

    public byte[] message() {
        return _message;
    }
    
    public ByteBuffer serialize() {
        ByteBuffer bb = ByteBuffer.allocate(_message.length+4);
        bb.putShort((short)_task);
        bb.putShort((short)_taskSrc);
        bb.put(_message);
        return bb;
    }
    
    public void deserialize(ByteBuffer packet) {
        if (packet==null) return;
        _task = packet.getShort();
        _taskSrc = packet.getShort();
        _message = new byte[packet.limit()-4];
        packet.get(_message);
    }

}
