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
package backtype.storm.tuple;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.GeneralTopologyContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Batch {
    public final int capacity;
    public final GeneralTopologyContext context;
    public final String sourceComponent;
    public final int sourceTaskId;
    public final String streamId;
    public final ArrayList<List<Object>> tupleBuffer;
    public final ArrayList<MessageId> idBuffer;


    public Batch(final int capacity, GeneralTopologyContext context, String sourceComponent, int sourceTaskId, String streamId) {
        assert (capacity > 0);
        assert (context != null);
        
        this.capacity = capacity;
        this.context = context;
        this.sourceComponent = sourceComponent;
        this.sourceTaskId = sourceTaskId;
        this.streamId = streamId;
        
        this.tupleBuffer = new ArrayList<List<Object>>(capacity);
        this.idBuffer = new ArrayList<MessageId>(capacity);
   }

    public void add(List<Object> tuple, MessageId id) {
        assert(this.tupleBuffer.size() < this.capacity);
        assert(this.tupleBuffer.size() == this.idBuffer.size());

        if(id == null) {
            id = MessageId.makeUnanchored();
        }

        this.tupleBuffer.add(tuple);
        this.idBuffer.add(id);
    }

    public int size() {
        assert(this.tupleBuffer.size() == this.idBuffer.size());
        return this.tupleBuffer.size();
    }

    public Batch newInstance() {
        return new Batch(this.capacity, this.context, this.sourceComponent, this.sourceTaskId, this.streamId);
    }

    public List<TupleImpl> getAsTupleList() {
        assert(this.tupleBuffer.size() == this.idBuffer.size());
        
        final int size = this.tupleBuffer.size();
        
        ArrayList<TupleImpl> result = new ArrayList<>(size);
        for(int i = 0; i < size; ++i) {
            result.add(new TupleImpl(context, this.tupleBuffer.get(i), sourceTaskId, this.streamId, this.idBuffer.get(i)));
        }
        
        return result;
    }
    
    public String toString() {
        assert(this.tupleBuffer.size() == this.idBuffer.size());
        String rc = "Batch(" + this.capacity + ") source: " + this.sourceComponent + " id: " + this.sourceTaskId + " stream: " + this.streamId + " data: [";
        for (int i = 0; i < this.tupleBuffer.size(); ++i) {
            rc += this.tupleBuffer.get(i) + "[" + this.idBuffer.get(i) + "]" + ", ";
        }
        return rc + "]";
    }

}
