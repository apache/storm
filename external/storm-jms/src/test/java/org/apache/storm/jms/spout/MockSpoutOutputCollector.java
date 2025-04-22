/*
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

package org.apache.storm.jms.spout;

import org.apache.storm.spout.ISpoutOutputCollector;

import java.util.ArrayList;
import java.util.List;

public class MockSpoutOutputCollector implements ISpoutOutputCollector {
    boolean emitted = false;

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        emitted = true;
        return new ArrayList<Integer>();
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emitted = true;
    }

    @Override
    public void flush() {
        //NO-OP
    }

    @Override
    public void reportError(Throwable error) {
    }

    public boolean emitted() {
        return this.emitted;
    }

    public void reset() {
        this.emitted = false;
    }

    @Override
    public long getPendingCount() {
        return 0;
    }
}
