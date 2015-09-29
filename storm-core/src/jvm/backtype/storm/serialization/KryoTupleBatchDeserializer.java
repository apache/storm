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
package backtype.storm.serialization;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.Batch;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.TupleImpl;
import com.esotericsoftware.kryo.io.Input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KryoTupleBatchDeserializer implements ITupleBatchDeserializer {
    GeneralTopologyContext _context;
    KryoValuesDeserializer _kryo;
    SerializationFactory.IdDictionary _ids;
    Input _kryoInput;

    public KryoTupleBatchDeserializer(final Map conf, final GeneralTopologyContext context) {
        _kryo = new KryoValuesDeserializer(conf);
        _context = context;
        _ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        _kryoInput = new Input(1);
    }

    public Object deserialize(byte[] ser) {
        try {
            _kryoInput.setBuffer(ser);
            byte header = _kryoInput.readByte();
            if(header == 'T') {
                int taskId = _kryoInput.readInt(true);
                int streamId = _kryoInput.readInt(true);
                MessageId id = MessageId.deserialize(_kryoInput);
                List<Object> values = _kryo.deserializeFrom(_kryoInput);

                String componentName = _context.getComponentId(taskId);
                String streamName = _ids.getStreamName(componentName, streamId);
                
                return new TupleImpl(_context, values, taskId, streamName, id);
            } else {
                assert (header == 'B');
                final int size = _kryoInput.readInt();
                int taskId = _kryoInput.readInt(true);
                int streamId = _kryoInput.readInt(true);

                List<TupleImpl> batch = new ArrayList<TupleImpl>(size);
                for(int i = 0; i < size; ++i) {
                    MessageId id = MessageId.deserialize(_kryoInput);
                    List<Object> values = _kryo.deserializeFrom(_kryoInput);

                    String componentName = _context.getComponentId(taskId);
                    String streamName = _ids.getStreamName(componentName, streamId);

                    batch.add(new TupleImpl(_context, values, taskId, streamName, id));
                }
                return batch;
            }
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

}
